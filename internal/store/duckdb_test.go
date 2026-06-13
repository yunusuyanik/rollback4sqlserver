package store

import (
	"testing"

	"github.com/uns/mssqllogrecovery/internal/dml"
)

func TestDeleteTransactionRemovesPersistedEvents(t *testing.T) {
	st, err := OpenDuckDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	err = st.Write(&dml.Statement{
		LSN:           "00000001:00000001:0001",
		TransactionID: "0000:00000001",
		Operation:     "DELETE",
		Table:         "[dbo].[T]",
		Database:      "testdb",
		SQL:           "DELETE FROM [dbo].[T];",
		RollbackSQL:   "INSERT INTO [dbo].[T] DEFAULT VALUES;",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := st.DeleteTransaction("testdb", "0000:00000001"); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM log_events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("event count=%d want 0", count)
	}
}

func TestWriteSkipsPhysicalCleanupStatement(t *testing.T) {
	st, err := OpenDuckDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	for i, rowHex := range []string{
		"070000000000000000bd2502000000",
		"4e0000000000000000bd2502000000",
	} {
		err := st.Write(&dml.Statement{
			LSN:                     "cleanup-" + string(rune('1'+i)),
			TransactionID:           "0000:000225be",
			Operation:               "DELETE",
			Table:                   "[dbo].[T]",
			Database:                "testdb",
			SQL:                     "-- compressed_row_parse_failed",
			CompressionDecodeStatus: "compressed_row_parse_failed",
			CompressedRowHex:        rowHex,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM log_events`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("event count=%d want 0", count)
	}
}
