package main

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/uns/mssqllogrecovery/internal/dml"
	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/store"
)

func TestIsInternalTransaction(t *testing.T) {
	internal := []string{
		"ShrinkFile",
		"AllocHeapPageSimpleXactDML",
		"AllocFirstPage",
		"QDS base transaction",
		"QDS nested transaction",
		"Backup:CommitLogArchivePoint",
	}
	for _, name := range internal {
		if !isInternalTransaction(name) {
			t.Errorf("isInternalTransaction(%q)=false", name)
		}
	}

	userTransactions := []string{"user_transaction", "INSERT", "UPDATE", "DELETE", ""}
	for _, name := range userTransactions {
		if isInternalTransaction(name) {
			t.Errorf("isInternalTransaction(%q)=true", name)
		}
	}
}

func TestIsPhysicalCleanupDelete(t *testing.T) {
	for _, row := range [][]byte{
		{0x07, 0, 0, 0, 0, 0, 0, 0, 0, 0xBD, 0x25, 0x02, 0, 0, 0},
		{0x4E, 0, 0, 0, 0, 0, 0, 0, 0, 0x6D, 0x24, 0x02, 0, 0, 0},
	} {
		rec := &logparser.LogRecord{
			Operation: logparser.OpDeleteRows,
			Contents0: row,
		}
		if !isPhysicalCleanupDelete(rec) {
			t.Errorf("cleanup row %x was not recognized", row)
		}
	}

	notCleanup := []*logparser.LogRecord{
		{Operation: logparser.OpInsertRows, Contents0: make([]byte, 15)},
		{Operation: logparser.OpDeleteRows, Contents0: []byte{0x07, 0, 0}},
		{Operation: logparser.OpDeleteRows, Contents0: []byte{0x07, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{Operation: logparser.OpDeleteRows, Contents0: []byte{0x23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, rec := range notCleanup {
		if isPhysicalCleanupDelete(rec) {
			t.Errorf("non-cleanup row %x was recognized", rec.Contents0)
		}
	}
}

func TestHandleTimelineBuckets(t *testing.T) {
	st, err := store.OpenDuckDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	base := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	// 3 events in bucket 0, 1 event ~6h later (bucket 36 of 72 over 12h).
	mk := func(lsn, op string, at time.Time) *dml.Statement {
		return &dml.Statement{
			LSN: lsn, TransactionID: "0000:0001", Operation: op, Table: "[dbo].[T]",
			Database: "db", SQL: op + " ...", Timestamp: at,
		}
	}
	for _, s := range []*dml.Statement{
		mk("00000001:00000001:0001", "INSERT", base.Add(1*time.Minute)),
		mk("00000001:00000001:0002", "UPDATE", base.Add(2*time.Minute)),
		mk("00000001:00000001:0003", "DELETE", base.Add(3*time.Minute)),
		mk("00000001:00000001:0004", "UPDATE", base.Add(6 * time.Hour)),
	} {
		if err := st.Write(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	appMu.Lock()
	appStore = st
	appMu.Unlock()
	defer func() { appMu.Lock(); appStore = nil; appMu.Unlock() }()

	since := base.Format(time.RFC3339)
	until := base.Add(12 * time.Hour).Format(time.RFC3339)
	req := httptest.NewRequest("GET", "/api/timeline?buckets=72&since="+since+"&until="+until, nil)
	rec := httptest.NewRecorder()
	handleTimeline(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Buckets int `json:"buckets"`
		Data    []struct {
			Insert, Update, Delete, Total int
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Data) != 72 {
		t.Fatalf("len(data)=%d want 72", len(resp.Data))
	}
	if got := resp.Data[0]; got.Insert != 1 || got.Update != 1 || got.Delete != 1 || got.Total != 3 {
		t.Fatalf("bucket0=%+v want ins1 upd1 del1 total3", got)
	}
	if got := resp.Data[36]; got.Update != 1 || got.Total != 1 {
		t.Fatalf("bucket36=%+v want upd1 total1", got)
	}
}

// Mirrors the exact browser call: fractional-second ISO timestamps, until=now,
// empty db/table/op filters. Guards against RFC3339 fractional-second parse or
// CAST(... 'Z' ...) regressions that would silently empty the timeline.
func TestHandleTimelineRealClientCall(t *testing.T) {
	st, err := store.OpenDuckDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	now := time.Now().UTC()
	for i, op := range []string{"INSERT", "UPDATE", "DELETE", "UPDATE"} {
		if err := st.Write(&dml.Statement{
			LSN: "00000001:00000001:000" + string(rune('1'+i)), TransactionID: "0000:0001",
			Operation: op, Table: "[dbo].[T]", Database: "db", SQL: op + " ...",
			Timestamp: now.Add(-time.Duration(i+1) * time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	appMu.Lock()
	appStore = st
	appMu.Unlock()
	defer func() { appMu.Lock(); appStore = nil; appMu.Unlock() }()

	// Fractional-second ISO, like new Date(..).toISOString().
	since := now.Add(-12 * time.Hour).Format("2006-01-02T15:04:05.000Z07:00")
	until := now.Format("2006-01-02T15:04:05.000Z07:00")
	u := "/api/timeline?buckets=72&db=&table=&op=&search=&since=" + since + "&until=" + until
	rec := httptest.NewRecorder()
	handleTimeline(rec, httptest.NewRequest("GET", u, nil))
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data []struct{ Insert, Update, Delete, Total int } `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, b := range resp.Data {
		total += b.Total
	}
	if total != 4 {
		t.Fatalf("total events=%d want 4 (timeline empty → bug)", total)
	}
}
