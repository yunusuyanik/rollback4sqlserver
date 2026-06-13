package mssql

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/uns/mssqllogrecovery/internal/schema"
)

func TestExtractCompressedColumnNibbles_EvenColumnCount(t *testing.T) {
	row := []byte{0x30, 0x00, 0x02, 0xB5}
	nibbles, payloadStart, err := ExtractCompressedColumnNibbles(row, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x2, 0x0, 0x5, 0xB}
	for i, w := range want {
		if nibbles[i] != w {
			t.Fatalf("nibble[%d]=%X want %X", i, nibbles[i], w)
		}
	}
	if payloadStart != 4 {
		t.Fatalf("payloadStart=%d want 4", payloadStart)
	}
}

func TestExtractCompressedColumnNibbles_OddColumnCount(t *testing.T) {
	row := []byte{0x30, 0x00, 0x21, 0x0A}
	nibbles, payloadStart, err := ExtractCompressedColumnNibbles(row, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x1, 0x2, 0xA}
	for i, w := range want {
		if nibbles[i] != w {
			t.Fatalf("nibble[%d]=%X want %X", i, nibbles[i], w)
		}
	}
	if payloadStart != 4 {
		t.Fatalf("payloadStart=%d want 4", payloadStart)
	}
}

func TestParseCompressedRowLayout_NullZeroInlineTailBit(t *testing.T) {
	rowHex := "3000025B7B54455354"
	row, _ := hex.DecodeString(rowHex)
	cols := []*schema.Column{
		{ColumnID: 1, Name: "OrderID", TypeID: schema.TypeInt, MaxLength: 4},
		{ColumnID: 2, Name: "CustomerID", TypeID: schema.TypeInt, MaxLength: 4, IsNullable: true},
		{ColumnID: 3, Name: "Active", TypeID: schema.TypeBit},
		{ColumnID: 4, Name: "OrderNo", TypeID: schema.TypeVarchar, MaxLength: 20, IsFixed: false},
	}
	for _, c := range cols {
		if c.TypeID == schema.TypeVarchar {
			c.IsFixed = false
		} else if c.TypeID != schema.TypeBit {
			c.IsFixed = true
			c.FixedOffset = 0
		}
	}

	slices, payloadStart, tailCols, err := ParseCompressedRowLayout(row, cols)
	if err != nil {
		t.Fatal(err)
	}
	if payloadStart != 4 {
		t.Fatalf("payloadStart=%d", payloadStart)
	}
	if len(tailCols) != 0 {
		t.Fatalf("unexpected tail columns: %v", tailCols)
	}
	if !slices[0].IsNull && slices[0].PhysicalLength != 1 {
		t.Fatalf("inline int slice=%+v", slices[0])
	}
	if !slices[1].IsNull {
		t.Fatalf("expected null customer")
	}
	if slices[2].Descriptor != 0xB {
		t.Fatalf("bit descriptor=%X", slices[2].Descriptor)
	}
	if slices[3].PhysicalLength != 4 {
		t.Fatalf("varchar physical=%d", slices[3].PhysicalLength)
	}
}

func TestUncompressSignedIntegerLike_Zero(t *testing.T) {
	out, err := UncompressSignedIntegerLike(nil, 4)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(out) != "00000000" {
		t.Fatalf("got %X", out)
	}
}

func TestUncompressSignedIntegerLike_IntPositive(t *testing.T) {
	// SQL Server compressed 123: MSB=1 (positive), 0xFB → flip→0x7B, LE pad → 123.
	out, err := UncompressSignedIntegerLike([]byte{0xFB}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(out) != "7b000000" {
		t.Fatalf("got %X", out)
	}
}

func TestUncompressSignedIntegerLike_IntNegative(t *testing.T) {
	// SQL Server compressed -1: MSB=0 (negative), 0x7F → flip→0xFF, FF-extend → -1.
	out, err := UncompressSignedIntegerLike([]byte{0x7F}, 4)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(out) != "ffffffff" {
		t.Fatalf("got %X", out)
	}
}

func TestUncompressFloatLike_Empty(t *testing.T) {
	out, err := UncompressFloatLike(nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 8 || out[0] != 0 {
		t.Fatalf("got %X", out)
	}
}

func TestUncompressBinary_PadsZeros(t *testing.T) {
	out := UncompressBinary([]byte{0x01, 0x02, 0x03}, 8)
	if hex.EncodeToString(out) != "0102030000000000" {
		t.Fatalf("got %X", out)
	}
}

func TestCompressedBitDescriptorB_IsTrue(t *testing.T) {
	sl := CompressedColumnSlice{Descriptor: 0xB, Column: &schema.Column{TypeID: schema.TypeBit}}
	val, err := decodeCompressedBit(sl)
	if err != nil {
		t.Fatal(err)
	}
	if val.IsNull || val.Raw != true {
		t.Fatalf("got %+v", val)
	}
}

func TestDecodeCompressedNVarChar_UnicodeCompression(t *testing.T) {
	// ASCII characters use one byte; U+0131 is encoded as 02 F1.
	compressed := []byte{0x41, 0x02, 0xF1, 0x42}
	value, raw, err := decodeCompressedVarChar(compressed, true, 20)
	if err != nil {
		t.Fatal(err)
	}
	if value != "AıB" {
		t.Fatalf("value=%q raw=%X", value, raw)
	}
}

func TestDecodeVarDecimal_SQLServerFixture(t *testing.T) {
	compressed, _ := hex.DecodeString("C3F6E8E5")
	value, _, err := decodeVarDecimal(compressed, 18, 4)
	if err != nil {
		t.Fatal(err)
	}
	if value != "9876.5432" {
		t.Fatalf("value=%s", value)
	}
}

func TestDecodePageCompressedHeap_SQLServerFixture(t *testing.T) {
	row, _ := hex.DecodeString("230FA2AA42158865A716E58A92D644C3F6E8E563552502C2490B80B46700A4CDD8433030314E43303031010203040506666972737401040010001B003C004C0011111111111111111111111111111111706167652D696E736572747061676520636F6D7072657373656420696E736572742061E702F16B6C616D61100102030405060708090A0B0C0D0E0F1000000000000000006D2402000000")
	table := &schema.Table{
		Schema: "LogRecoveryTest", Name: "PageCompressed_Heap_15Cols",
		DataCompression: schema.CompressionPage,
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "Id", TypeID: schema.TypeInt, MaxLength: 4},
			{ColumnID: 2, Name: "EntityId", TypeID: schema.TypeUniqueidentifier, MaxLength: 16},
			{ColumnID: 3, Name: "Name", TypeID: schema.TypeVarchar, MaxLength: 100},
			{ColumnID: 4, Name: "Description", TypeID: schema.TypeNvarchar, MaxLength: 400},
			{ColumnID: 5, Name: "Qty", TypeID: schema.TypeInt, MaxLength: 4},
			{ColumnID: 6, Name: "Amount", TypeID: schema.TypeMoney, MaxLength: 8},
			{ColumnID: 7, Name: "Price", TypeID: schema.TypeDecimal, MaxLength: 9, Precision: 18, Scale: 4},
			{ColumnID: 8, Name: "IsCompleted", TypeID: schema.TypeBit, MaxLength: 1},
			{ColumnID: 9, Name: "AuditCreateDate", TypeID: schema.TypeDatetime2, MaxLength: 7, Scale: 3},
			{ColumnID: 10, Name: "AuditUpdateDate", TypeID: schema.TypeDatetime, MaxLength: 8},
			{ColumnID: 11, Name: "Code", TypeID: schema.TypeChar, MaxLength: 10},
			{ColumnID: 12, Name: "NCode", TypeID: schema.TypeNchar, MaxLength: 20},
			{ColumnID: 13, Name: "Payload", TypeID: schema.TypeVarbinary, MaxLength: 100},
			{ColumnID: 14, Name: "FixedPayload", TypeID: schema.TypeBinary, MaxLength: 16},
			{ColumnID: 15, Name: "Note", TypeID: schema.TypeVarchar, MaxLength: 100},
		},
	}

	decoded, err := (&MSSQLCompressedRowDecoder{}).Decode(row, table, CompressionPage)
	if err != nil {
		t.Fatal(err)
	}
	checks := map[int]interface{}{
		0:  int64(101),
		2:  "page-insert",
		3:  "page compressed insert açıklama",
		4:  int64(10),
		5:  "123.4500",
		6:  "9876.5432",
		7:  false,
		10: "C001",
		11: "NC001",
		14: "first",
	}
	for i, want := range checks {
		if got := decoded.Values[i].Raw; got != want {
			t.Fatalf("column %d=%v want %v", i, got, want)
		}
	}
	if got := decoded.Values[9].Raw.(time.Time).Format("2006-01-02 15:04:05.000"); got != "2026-06-12 10:00:02.000" {
		t.Fatalf("AuditUpdateDate=%s debug=%+v", got, decoded.CompressionDebug.Columns[9])
	}
}

func TestDecodeCompressedRow_PageReferenceRequiresPageDictionary(t *testing.T) {
	row, _ := hex.DecodeString("30000C")
	table := &schema.Table{
		Schema:          "dbo",
		Name:            "PageCompressed",
		DataCompression: schema.CompressionPage,
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "Value", TypeID: schema.TypeVarchar, MaxLength: 20},
		},
	}

	decoded, err := (&MSSQLCompressedRowDecoder{}).Decode(row, table, CompressionPage)
	if !errors.Is(err, ErrCompressedPageReference) {
		t.Fatalf("error=%v, want ErrCompressedPageReference", err)
	}
	if decoded.CompressionDebug.Warning == "" {
		t.Fatal("expected page dictionary diagnostic")
	}
}

func TestDecodeCompressedOrderRow(t *testing.T) {
	rowHex := "30000216FB4F52442D31"
	row, _ := hex.DecodeString(rowHex)
	table := &schema.Table{
		ObjectID:        100,
		Schema:          "dbo",
		Name:            "Order",
		DataCompression: schema.CompressionRow,
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "OrderID", TypeID: schema.TypeInt, MaxLength: 4},
			{ColumnID: 2, Name: "CustomerID", TypeID: schema.TypeInt, MaxLength: 4, IsNullable: true},
			{ColumnID: 3, Name: "OrderNo", TypeID: schema.TypeVarchar, MaxLength: 20},
			{ColumnID: 4, Name: "Amount", TypeID: schema.TypeMoney, MaxLength: 8, IsNullable: true},
		},
	}

	decoder := &MSSQLCompressedRowDecoder{}
	decoded, err := decoder.Decode(row, table, CompressionRow)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Values[0].Raw != int64(123) {
		t.Fatalf("OrderID=%v", decoded.Values[0].Raw)
	}
	if !decoded.Values[1].IsNull {
		t.Fatalf("CustomerID should be null")
	}
	if decoded.Values[2].Raw != "ORD-1" {
		t.Fatalf("OrderNo=%v", decoded.Values[2].Raw)
	}
	if decoded.Values[3].Raw != "0.0000" {
		t.Fatalf("Amount=%v", decoded.Values[3].Raw)
	}
}
