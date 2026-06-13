package dml_test

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/uns/mssqllogrecovery/internal/dml"
	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/schema"
	"github.com/uns/mssqllogrecovery/internal/store"
)

func compressedOrderTable() *schema.Table {
	return &schema.Table{
		ObjectID:        100,
		Schema:          "dbo",
		Name:            "Order",
		DataCompression: schema.CompressionRow,
		PKCols:          []int{1},
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "OrderID", TypeID: schema.TypeInt, MaxLength: 4, IsFixed: true, FixedOffset: 0},
			{ColumnID: 2, Name: "CustomerID", TypeID: schema.TypeInt, MaxLength: 4, IsNullable: true, IsFixed: true, FixedOffset: 4},
			{ColumnID: 3, Name: "OrderNo", TypeID: schema.TypeVarchar, MaxLength: 20, IsFixed: false, VarIndex: 0},
			{ColumnID: 4, Name: "Amount", TypeID: schema.TypeMoney, MaxLength: 8, IsNullable: true, IsFixed: true, FixedOffset: 8},
		},
	}
}

func TestDecodeCompressedDeleteRow_BuildsDeleteAndUndoInsert(t *testing.T) {
	row, _ := hex.DecodeString("30000216FB4F52442D31")
	table := compressedOrderTable()
	sch := &schema.Schema{
		Tables:                 map[int]*schema.Table{table.ObjectID: table},
		ByName:                 map[string]*schema.Table{"dbo.Order": table},
		CompressionByAllocUnit: map[int64]*schema.PartitionCompression{},
		CompressionByPartition: map[int64]*schema.PartitionCompression{},
	}
	gen := dml.New(sch)
	rec := &logparser.LogRecord{
		LSN:           "000001:000001:0001",
		Operation:     logparser.OpDeleteRows,
		Context:       "LCX_CLUSTERED",
		TransactionID: "0000:000001",
		AllocUnitName: "dbo.Order",
		Contents0:     row,
	}
	stmt, err := gen.Generate(rec)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.CompressionDecodeStatus != "ok" {
		t.Fatalf("compression_decode_status=%q", stmt.CompressionDecodeStatus)
	}
	if !strings.Contains(stmt.SQL, "DELETE FROM [dbo].[Order]") {
		t.Fatalf("SQL=%q", stmt.SQL)
	}
	if !strings.Contains(stmt.SQL, "[OrderID] = 123") {
		t.Fatalf("SQL=%q", stmt.SQL)
	}
	if !strings.Contains(stmt.RollbackSQL, "INSERT INTO [dbo].[Order]") {
		t.Fatalf("RollbackSQL=%q", stmt.RollbackSQL)
	}
	if strings.Contains(stmt.SQL, "COMPRESSED_ROW_NOT_SUPPORTED") {
		t.Fatal("unexpected COMPRESSED_ROW_NOT_SUPPORTED")
	}
}

func TestCompressedDelete_MetadataMissing_WritesPartialDuckDBEvent(t *testing.T) {
	row, _ := hex.DecodeString("30000216FB4F52442D31")
	table := compressedOrderTable()
	table.DataCompression = schema.CompressionNone

	sch := &schema.Schema{
		Tables:                 map[int]*schema.Table{table.ObjectID: table},
		ByName:                 map[string]*schema.Table{"dbo.Order": table},
		CompressionByAllocUnit: map[int64]*schema.PartitionCompression{},
		CompressionByPartition: map[int64]*schema.PartitionCompression{},
	}
	gen := dml.New(sch)
	rec := &logparser.LogRecord{
		LSN:           "000001:000002:0001",
		Operation:     logparser.OpDeleteRows,
		Context:       "LCX_CLUSTERED",
		AllocUnitName: "dbo.Order",
		AllocUnitID:   1234,
		PartitionID:   5678,
		PageID:        "0001:0000002a",
		Contents0:     row,
		Contents1:     []byte{0xAA, 0xBB},
		RawLogRecord:  []byte{0xCC, 0xDD},
	}
	slotID := 3
	rec.SlotID = &slotID
	stmt, err := gen.Generate(rec)
	if err != nil {
		t.Fatal(err)
	}
	if stmt.CompressionDecodeStatus != "compressed_row_parse_failed" {
		t.Fatalf("expected safe metadata failure, got status=%q", stmt.CompressionDecodeStatus)
	}
	var debug map[string]interface{}
	if err := json.Unmarshal([]byte(stmt.DecompressedDebugJSON), &debug); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]interface{}{
		"context":            "LCX_CLUSTERED",
		"page_id":            "0001:0000002a",
		"rowlog1_hex":        "aabb",
		"raw_log_record_hex": "ccdd",
	} {
		if got := debug[key]; got != want {
			t.Fatalf("debug[%q]=%v want %v", key, got, want)
		}
	}

	st, err := store.OpenDuckDB("")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stmt.Database = "testdb"
	if err := st.Write(stmt); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	var compressedHex string
	err = st.DB().QueryRow(`SELECT compressed_row_hex FROM log_events WHERE lsn = ?`, stmt.LSN).Scan(&compressedHex)
	if err != nil {
		t.Fatal(err)
	}
	if compressedHex == "" {
		t.Fatal("expected compressed_row_hex in DuckDB")
	}
}

func TestRowCompressedModifyRow_ReconstructsFullImagesFromCache(t *testing.T) {
	table := &schema.Table{
		ObjectID:        200,
		Schema:          "LogRecoveryTest",
		Name:            "RowCompressed_Clustered",
		BaseIndexName:   "PK_RowCompressed_Clustered",
		DataCompression: schema.CompressionRow,
		PKCols:          []int{1, 2},
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "Id", TypeID: schema.TypeInt, MaxLength: 4},
			{ColumnID: 2, Name: "TenantId", TypeID: schema.TypeInt, MaxLength: 4},
			{ColumnID: 3, Name: "Name", TypeID: schema.TypeVarchar, MaxLength: 100},
			{ColumnID: 4, Name: "Qty", TypeID: schema.TypeInt, MaxLength: 4},
			{ColumnID: 5, Name: "Amount", TypeID: schema.TypeMoney, MaxLength: 8},
			{ColumnID: 6, Name: "CreatedAt", TypeID: schema.TypeDatetime, MaxLength: 8},
			{ColumnID: 7, Name: "UpdatedAt", TypeID: schema.TypeDatetime2, MaxLength: 7, Scale: 3},
			{ColumnID: 8, Name: "IsActive", TypeID: schema.TypeBit, MaxLength: 1},
			{ColumnID: 9, Name: "Payload", TypeID: schema.TypeVarbinary, MaxLength: 100},
		},
	}
	sch := &schema.Schema{
		Tables:                 map[int]*schema.Table{table.ObjectID: table},
		ByName:                 map[string]*schema.Table{"LogRecoveryTest.RowCompressed_Clustered": table},
		CompressionByAllocUnit: map[int64]*schema.PartitionCompression{},
		CompressionByPartition: map[int64]*schema.PartitionCompression{},
		PartitionPhysicalCols:  map[int64][]*schema.Column{},
	}
	gen := dml.New(sch)
	slot := 0
	insertRow, _ := hex.DecodeString("2309232A84B81780C9879EA1ECC480B46700C5C100B0339302C2490B1122334455660101000A00726F772D696E7365727400000000000000006D2402000000")
	oldFrag, _ := hex.DecodeString("B81780C9879EA1ECC480B46700C5C100B0339302C2490B1122334455660101000A00726F772D696E73657274")
	newFrag, _ := hex.DecodeString("181580C987A8B2E10080B46700C5C1003D39D602C2490B998877660101000B00726F772D75706461746564")

	insert := &logparser.LogRecord{
		LSN: "1", Operation: logparser.OpInsertRows, Context: "LCX_CLUSTERED",
		AllocUnitName: "LogRecoveryTest.RowCompressed_Clustered.PK_RowCompressed_Clustered",
		AllocUnitID:   1, PageID: "0001:00000330", SlotID: &slot, Contents0: insertRow,
	}
	if _, err := gen.Generate(insert); err != nil {
		t.Fatal(err)
	}

	update := &logparser.LogRecord{
		LSN: "2", Operation: logparser.OpModifyRow, Context: "LCX_CLUSTERED",
		AllocUnitName: "LogRecoveryTest.RowCompressed_Clustered.PK_RowCompressed_Clustered",
		AllocUnitID:   1, PageID: "0001:00000330", SlotID: &slot,
		OffsetInRow: 5, Contents0: oldFrag, Contents1: newFrag,
	}
	stmt, err := gen.Generate(update)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"[Name] = 'row-updated'",
		"[Qty] = 40",
		"[Amount] = 333.4400",
		"[UpdatedAt] = '2026-06-12 13:13:13.7890000'",
		"WHERE [Id] = 201 AND [TenantId] = 7",
	} {
		if !strings.Contains(stmt.SQL, want) {
			t.Fatalf("SQL missing %q: %s", want, stmt.SQL)
		}
	}
	if !strings.Contains(stmt.RollbackSQL, "[Name] = 'row-insert'") {
		t.Fatalf("rollback=%s", stmt.RollbackSQL)
	}
}
