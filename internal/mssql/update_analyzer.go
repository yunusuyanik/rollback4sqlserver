// Package mssql implements SQL Server transaction log UPDATE analysis.
// It decodes LOP_MODIFY_ROW and LOP_MODIFY_COLUMNS records into structured
// before/after events with equivalent redo and undo SQL, and persists them
// via the UpdateStore interface.
package mssql

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// Status values for UpdateEvent.Status.
const (
	StatusOK                          = "ok"
	StatusMR1NotFound                 = "mr1_not_found"
	StatusPatchNotFound               = "patch_not_found"
	StatusAmbiguousPatchLocation      = "ambiguous_patch_location"
	StatusVariableLengthPatchRequired = "variable_length_patch_required"
	StatusDecodeFailed                = "decode_failed"
	StatusWhereNotBuildable           = "where_not_buildable"
	StatusUnsupportedModifyColumns    = "unsupported_modify_columns_format"
	StatusNoChangedColumns            = "no_changed_columns"
)

// Confidence values for UpdateEvent.Confidence.
const (
	ConfidenceHigh   = "high"
	ConfidenceMedium = "medium"
	ConfidenceLow    = "low"
)

// UpdateEvent holds all data for one UPDATE log record. Written to UpdateStore
// regardless of success so that no events are silently dropped.
type UpdateEvent struct {
	SourceDB      string
	SchemaName    string
	TableName     string
	CurrentLSN    string
	TransactionID string
	Operation     string
	Context       string
	PageID        string
	SlotID        *int
	AllocUnitID   int64
	PartitionID   int64

	RowLog0Hex string
	RowLog1Hex string
	RowLog2Hex string
	RowLog3Hex string

	MR1Hex string
	MR0Hex string

	BeforeJSON         string
	AfterJSON          string
	ChangedColumnsJSON string

	EquivalentRedoSQL string
	EquivalentUndoSQL string

	Status       string
	Confidence   string
	ErrorMessage string
	DebugJSON    string

	CreatedAt time.Time
}

// DebugInfo is marshalled into UpdateEvent.DebugJSON.
type DebugInfo struct {
	PatchOffset   int    `json:"patch_offset,omitempty"`
	PKProbeHex    string `json:"pk_probe_hex,omitempty"`
	RowLog0Len    int    `json:"rowlog0_len"`
	RowLog1Len    int    `json:"rowlog1_len"`
	MR1Source     string `json:"mr1_source,omitempty"`
	DecoderName   string `json:"decoder_name"`
	WhereStrategy string `json:"where_strategy,omitempty"`
	Err           string `json:"error,omitempty"`
}

// UpdateStore persists UPDATE events.
type UpdateStore interface {
	WriteUpdateEvent(ctx context.Context, evt UpdateEvent) error
}

// AnalyzeUpdateRecord decodes one LOP_MODIFY_ROW or LOP_MODIFY_COLUMNS record
// and writes the result to duck. Always writes an event — never silently drops.
// Returns nil for non-UPDATE operations.
func AnalyzeUpdateRecord(
	ctx context.Context,
	rec logparser.LogRecord,
	meta *schema.Table,
	cache *RowImageCache,
	pageReader PageReader,
	decoder UpdateDecoder,
	duck UpdateStore,
	sourceDB string,
) error {
	if rec.Operation != logparser.OpModifyRow && rec.Operation != logparser.OpModifyColumns {
		return nil
	}

	evt := UpdateEvent{
		SourceDB:      sourceDB,
		SchemaName:    meta.Schema,
		TableName:     meta.Name,
		CurrentLSN:    rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     rec.Operation,
		Context:       rec.Context,
		PageID:        rec.PageID,
		SlotID:        rec.SlotID,
		AllocUnitID:   rec.AllocUnitID,
		PartitionID:   rec.PartitionID,
		RowLog0Hex:    toUpperHex(rec.Contents0),
		RowLog1Hex:    toUpperHex(rec.Contents1),
		RowLog2Hex:    toUpperHex(rec.Contents2),
		RowLog3Hex:    toUpperHex(rec.Contents3),
		CreatedAt:     time.Now().UTC(),
	}

	dbg := DebugInfo{
		RowLog0Len:  len(rec.Contents0),
		RowLog1Len:  len(rec.Contents1),
		DecoderName: "StandardRowDecoder",
	}

	pkProbe := ExtractPrimaryKeyProbeFromRowLog2(rec.Contents2)
	dbg.PKProbeHex = pkProbe

	// Step 1: resolve MR1 (after-image).
	mr1, mr1Source, err := ResolveMR1(ctx, rec, pkProbe, cache, pageReader)
	if err != nil {
		dbg.MR1Source = "none"
		dbg.Err = err.Error()
		evt.Status = StatusMR1NotFound
		evt.Confidence = ConfidenceLow
		evt.ErrorMessage = err.Error()
		evt.DebugJSON = mustMarshal(dbg)
		return duck.WriteUpdateEvent(ctx, evt)
	}
	dbg.MR1Source = mr1Source
	evt.MR1Hex = toUpperHex(mr1)

	// Step 2: reconstruct MR0 (before-image) by reverse-patching.
	mr0, patchOffset, err := BuildBeforeImageFromUpdateLog(rec, mr1, meta)
	if err != nil {
		status := patchErrStatus(err)
		if rec.Operation == logparser.OpModifyColumns {
			status = StatusUnsupportedModifyColumns
		}
		dbg.Err = err.Error()
		evt.Status = status
		evt.Confidence = ConfidenceLow
		evt.ErrorMessage = err.Error()
		evt.DebugJSON = mustMarshal(dbg)
		return duck.WriteUpdateEvent(ctx, evt)
	}
	dbg.PatchOffset = patchOffset
	evt.MR0Hex = toUpperHex(mr0)

	// Step 3: decode after-image then before-image.
	afterRow, err := decoder.Decode(mr1, meta)
	if err != nil {
		dbg.Err = fmt.Sprintf("after decode: %v", err)
		evt.Status = StatusDecodeFailed
		evt.Confidence = ConfidenceLow
		evt.ErrorMessage = dbg.Err
		evt.DebugJSON = mustMarshal(dbg)
		return duck.WriteUpdateEvent(ctx, evt)
	}
	beforeRow, err := decoder.Decode(mr0, meta)
	if err != nil {
		dbg.Err = fmt.Sprintf("before decode: %v", err)
		evt.Status = StatusDecodeFailed
		evt.Confidence = ConfidenceLow
		evt.ErrorMessage = dbg.Err
		evt.DebugJSON = mustMarshal(dbg)
		return duck.WriteUpdateEvent(ctx, evt)
	}

	evt.BeforeJSON = mustMarshal(rowToJSONMap(beforeRow))
	evt.AfterJSON = mustMarshal(rowToJSONMap(afterRow))

	// Step 4: diff.
	changed := DiffRows(beforeRow, afterRow, meta)
	if len(changed) == 0 {
		evt.Status = StatusNoChangedColumns
		evt.Confidence = ConfidenceLow
		evt.ChangedColumnsJSON = "[]"
		evt.DebugJSON = mustMarshal(dbg)
		return duck.WriteUpdateEvent(ctx, evt)
	}
	evt.ChangedColumnsJSON = mustMarshal(changedToJSON(changed))

	// Step 5: build equivalent SQL.
	redo, undo, sqlErr := BuildUpdateSQL(meta.Schema, meta.Name, changed, beforeRow, afterRow, meta)
	if sqlErr != nil {
		dbg.Err = sqlErr.Error()
		evt.Status = StatusWhereNotBuildable
		evt.Confidence = ConfidenceLow
		evt.ErrorMessage = sqlErr.Error()
		evt.DebugJSON = mustMarshal(dbg)
		return duck.WriteUpdateEvent(ctx, evt)
	}

	evt.EquivalentRedoSQL = redo
	evt.EquivalentUndoSQL = undo

	_, whereStrategy, _ := BuildWherePredicate(beforeRow, meta)
	dbg.WhereStrategy = whereStrategy
	switch whereStrategy {
	case "primary_key":
		evt.Confidence = ConfidenceHigh
	default:
		evt.Confidence = ConfidenceMedium
	}
	evt.Status = StatusOK
	evt.DebugJSON = mustMarshal(dbg)
	return duck.WriteUpdateEvent(ctx, evt)
}

func patchErrStatus(err error) string {
	switch {
	case errors.Is(err, ErrAmbiguousPatch):
		return StatusAmbiguousPatchLocation
	case errors.Is(err, ErrVariableLengthPatchRequired):
		return StatusVariableLengthPatchRequired
	case errors.Is(err, ErrPatchNotFound):
		return StatusPatchNotFound
	}
	return StatusDecodeFailed
}

func toUpperHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return strings.ToUpper(hex.EncodeToString(b))
}

func mustMarshal(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func rowToJSONMap(row UpdateRow) map[string]any {
	m := make(map[string]any, len(row.Values))
	for k, v := range row.Values {
		if v.IsNull {
			m[k] = nil
		} else {
			m[k] = v.GoValue
		}
	}
	return m
}

func changedToJSON(changed []ChangedColumn) []map[string]any {
	out := make([]map[string]any, len(changed))
	for i, cc := range changed {
		out[i] = map[string]any{
			"column": cc.ColumnName,
			"type":   cc.TypeName,
			"before": cc.Before.SQLLiteral,
			"after":  cc.After.SQLLiteral,
		}
	}
	return out
}
