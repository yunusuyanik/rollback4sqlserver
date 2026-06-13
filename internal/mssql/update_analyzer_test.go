package mssql

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// --- mocks ---

type captureStore struct {
	events []UpdateEvent
}

func (s *captureStore) WriteUpdateEvent(_ context.Context, evt UpdateEvent) error {
	s.events = append(s.events, evt)
	return nil
}

type funcPageReader struct {
	fn func(context.Context, logparser.LogRecord, string) ([]byte, error)
}

func (p funcPageReader) ReadCurrentRowImage(ctx context.Context, rec logparser.LogRecord, probe string) ([]byte, error) {
	return p.fn(ctx, rec, probe)
}

type callCountDecoder struct {
	calls  int
	after  UpdateRow
	before UpdateRow
}

func (d *callCountDecoder) Decode(_ []byte, _ *schema.Table) (UpdateRow, error) {
	d.calls++
	if d.calls == 1 {
		return d.after, nil
	}
	return d.before, nil
}

// --- tests ---

func TestAnalyzeUpdateRecord_MR1NotFound_WritesPartialDuckDBEvent(t *testing.T) {
	meta := &schema.Table{
		Schema:  "dbo",
		Name:    "Orders",
		PKCols:  []int{1},
		Columns: []*schema.Column{{ColumnID: 1, Name: "ID", TypeID: schema.TypeInt, IsFixed: true}},
	}

	rec := logparser.LogRecord{
		LSN:          "0000:0001:0001",
		Operation:    logparser.OpModifyRow,
		Context:      "LCX_CLUSTERED",
		TransactionID: "0x0001",
		Contents0:    []byte{0x10, 0x20},
		Contents1:    []byte{0x11, 0x22},
	}

	store := &captureStore{}
	err := AnalyzeUpdateRecord(
		context.Background(),
		rec, meta,
		NewRowImageCache(),
		NopPageReader{},
		StandardRowDecoder{},
		store,
		"testdb",
	)
	if err != nil {
		t.Fatalf("AnalyzeUpdateRecord returned error: %v", err)
	}
	if len(store.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(store.events))
	}
	evt := store.events[0]
	if evt.Status != StatusMR1NotFound {
		t.Errorf("status: got %q, want %q", evt.Status, StatusMR1NotFound)
	}
	if evt.Confidence != ConfidenceLow {
		t.Errorf("confidence: got %q, want %q", evt.Confidence, ConfidenceLow)
	}
	if evt.ErrorMessage == "" {
		t.Error("ErrorMessage should be non-empty")
	}
	// Raw hex must be populated.
	if evt.RowLog0Hex != "1020" {
		t.Errorf("RowLog0Hex: got %q, want \"1020\"", evt.RowLog0Hex)
	}
}

func TestAnalyzeUpdateRecord_NonUpdateOp_ReturnsNil(t *testing.T) {
	meta := &schema.Table{Schema: "dbo", Name: "T", Columns: []*schema.Column{}}
	rec := logparser.LogRecord{Operation: logparser.OpInsertRows}
	store := &captureStore{}
	err := AnalyzeUpdateRecord(context.Background(), rec, meta,
		NewRowImageCache(), NopPageReader{}, StandardRowDecoder{}, store, "db")
	if err != nil {
		t.Fatalf("expected nil for non-update op: %v", err)
	}
	if len(store.events) != 0 {
		t.Error("no event should be written for non-update op")
	}
}

func TestAnalyzeUpdateRecord_OK_WritesRedoUndoAndJson(t *testing.T) {
	meta := &schema.Table{
		Schema:  "dbo",
		Name:    "Test",
		PKCols:  []int{1},
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "ID", TypeID: schema.TypeInt, IsFixed: true},
			{ColumnID: 2, Name: "Val", TypeID: schema.TypeInt, IsFixed: true},
		},
	}

	// MR1 contains Contents0 at offset 4.
	// PatchBytesReverse finds [0x10,0x20,0x30,0x40] in MR1 at offset 4 and replaces
	// with Contents1 [0x01,0x02,0x03,0x04] to produce MR0.
	mr1 := []byte{0x00, 0x00, 0xAA, 0xBB, 0x10, 0x20, 0x30, 0x40, 0xCC, 0xDD}

	rec := logparser.LogRecord{
		LSN:           "0000:0002:0001",
		Operation:     logparser.OpModifyRow,
		Context:       "LCX_CLUSTERED",
		TransactionID: "0x0002",
		Contents0:     []byte{0x10, 0x20, 0x30, 0x40}, // new fragment (in MR1 at offset 4)
		Contents1:     []byte{0x01, 0x02, 0x03, 0x04}, // old fragment
	}

	sqlVal := func(name string, v int64) SqlValue {
		return SqlValue{
			ColumnName: name, TypeName: "int",
			GoValue: v, SQLLiteral: fmt.Sprintf("%d", v),
		}
	}
	afterRow := UpdateRow{Values: map[string]SqlValue{
		"ID":  sqlVal("ID", 1),
		"Val": sqlVal("Val", 200),
	}}
	beforeRow := UpdateRow{Values: map[string]SqlValue{
		"ID":  sqlVal("ID", 1),
		"Val": sqlVal("Val", 100),
	}}

	dec := &callCountDecoder{after: afterRow, before: beforeRow}
	pr := funcPageReader{fn: func(_ context.Context, _ logparser.LogRecord, _ string) ([]byte, error) {
		return mr1, nil
	}}

	store := &captureStore{}
	err := AnalyzeUpdateRecord(context.Background(), rec, meta,
		NewRowImageCache(), pr, dec, store, "testdb")
	if err != nil {
		t.Fatalf("AnalyzeUpdateRecord error: %v", err)
	}
	if len(store.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(store.events))
	}
	evt := store.events[0]

	if evt.Status != StatusOK {
		t.Errorf("status: got %q, want %q", evt.Status, StatusOK)
	}
	if evt.Confidence != ConfidenceHigh {
		t.Errorf("confidence: got %q, want %q", evt.Confidence, ConfidenceHigh)
	}

	wantRedo := "update top(1) [dbo].[Test] set [Val]=200 where [ID]=1;"
	wantUndo := "update top(1) [dbo].[Test] set [Val]=100 where [ID]=1;"
	if evt.EquivalentRedoSQL != wantRedo {
		t.Errorf("redo:\n  got  %q\n  want %q", evt.EquivalentRedoSQL, wantRedo)
	}
	if evt.EquivalentUndoSQL != wantUndo {
		t.Errorf("undo:\n  got  %q\n  want %q", evt.EquivalentUndoSQL, wantUndo)
	}

	// MR0 hex should be MR1 with patch applied.
	wantMR0 := "0000AABB01020304CCDD"
	if evt.MR0Hex != wantMR0 {
		t.Errorf("MR0Hex: got %q, want %q", evt.MR0Hex, wantMR0)
	}

	// changed_columns_json should contain Val.
	var cols []map[string]any
	if err := json.Unmarshal([]byte(evt.ChangedColumnsJSON), &cols); err != nil {
		t.Fatalf("ChangedColumnsJSON invalid: %v", err)
	}
	if len(cols) != 1 || cols[0]["column"] != "Val" {
		t.Errorf("ChangedColumnsJSON: unexpected content: %s", evt.ChangedColumnsJSON)
	}

	// before_json and after_json must be non-empty objects.
	if evt.BeforeJSON == "" || evt.BeforeJSON == "{}" {
		t.Error("BeforeJSON should be populated")
	}
	if evt.AfterJSON == "" || evt.AfterJSON == "{}" {
		t.Error("AfterJSON should be populated")
	}
}

func TestAnalyzeUpdateRecord_AmbiguousPatch_WritesEvent(t *testing.T) {
	meta := &schema.Table{
		Schema:  "dbo",
		Name:    "T",
		PKCols:  []int{1},
		Columns: []*schema.Column{{ColumnID: 1, Name: "ID", TypeID: schema.TypeInt, IsFixed: true}},
	}

	// MR1 has [0xAA, 0xBB] at two locations → ambiguous patch.
	mr1 := []byte{0xAA, 0xBB, 0x00, 0xAA, 0xBB}

	rec := logparser.LogRecord{
		LSN:       "0000:0003:0001",
		Operation: logparser.OpModifyRow,
		Contents0: []byte{0xAA, 0xBB},
		Contents1: []byte{0x11, 0x22},
	}

	pr := funcPageReader{fn: func(_ context.Context, _ logparser.LogRecord, _ string) ([]byte, error) {
		return mr1, nil
	}}

	store := &captureStore{}
	AnalyzeUpdateRecord(context.Background(), rec, meta,
		NewRowImageCache(), pr, StandardRowDecoder{}, store, "db")

	if len(store.events) != 1 {
		t.Fatalf("expected 1 event")
	}
	if store.events[0].Status != StatusAmbiguousPatchLocation {
		t.Errorf("status: got %q, want %q", store.events[0].Status, StatusAmbiguousPatchLocation)
	}
}
