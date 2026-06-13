package mssql

import (
	"fmt"
	"testing"

	"github.com/uns/mssqllogrecovery/internal/schema"
)

// helpers

func sv(name string, typeID int, v int64) SqlValue {
	return SqlValue{
		ColumnName: name,
		TypeName:   sqlTypeName(&schema.Column{TypeID: typeID}),
		GoValue:    v,
		SQLLiteral: fmt.Sprintf("%d", v),
	}
}

func makeTable(schemaName, tableName string, pkColIDs []int, cols ...*schema.Column) *schema.Table {
	return &schema.Table{Schema: schemaName, Name: tableName, PKCols: pkColIDs, Columns: cols}
}

func fixedCol(id int, name string, typeID int) *schema.Column {
	return &schema.Column{ColumnID: id, Name: name, TypeID: typeID, IsFixed: true}
}

func buildRow(pairs ...any) UpdateRow {
	dr := UpdateRow{Values: make(map[string]SqlValue)}
	for i := 0; i+1 < len(pairs); i += 2 {
		name := pairs[i].(string)
		val := pairs[i+1].(SqlValue)
		dr.Values[name] = val
	}
	return dr
}

// tests

func TestBuildUpdateSQL_PKNotChanged(t *testing.T) {
	meta := makeTable("dbo", "Orders", []int{1},
		fixedCol(1, "OrderID", schema.TypeInt),
		fixedCol(2, "QTY", schema.TypeInt),
	)

	before := buildRow("OrderID", sv("OrderID", schema.TypeInt, 1001), "QTY", sv("QTY", schema.TypeInt, 100))
	after := buildRow("OrderID", sv("OrderID", schema.TypeInt, 1001), "QTY", sv("QTY", schema.TypeInt, 999))

	changed := []ChangedColumn{
		{ColumnName: "QTY", TypeName: "int", Before: sv("QTY", schema.TypeInt, 100), After: sv("QTY", schema.TypeInt, 999)},
	}

	redo, undo, err := BuildUpdateSQL("dbo", "Orders", changed, before, after, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantRedo := "update top(1) [dbo].[Orders] set [QTY]=999 where [OrderID]=1001;"
	wantUndo := "update top(1) [dbo].[Orders] set [QTY]=100 where [OrderID]=1001;"

	if redo != wantRedo {
		t.Errorf("redo:\n  got  %q\n  want %q", redo, wantRedo)
	}
	if undo != wantUndo {
		t.Errorf("undo:\n  got  %q\n  want %q", undo, wantUndo)
	}
}

func TestBuildUpdateSQL_PKChanged(t *testing.T) {
	meta := makeTable("dbo", "Test", []int{1},
		fixedCol(1, "ID", schema.TypeInt),
		fixedCol(2, "Val", schema.TypeInt),
	)

	before := buildRow("ID", sv("ID", schema.TypeInt, 1), "Val", sv("Val", schema.TypeInt, 10))
	after := buildRow("ID", sv("ID", schema.TypeInt, 2), "Val", sv("Val", schema.TypeInt, 20))

	changed := []ChangedColumn{
		{ColumnName: "ID", TypeName: "int", Before: sv("ID", schema.TypeInt, 1), After: sv("ID", schema.TypeInt, 2)},
		{ColumnName: "Val", TypeName: "int", Before: sv("Val", schema.TypeInt, 10), After: sv("Val", schema.TypeInt, 20)},
	}

	redo, undo, err := BuildUpdateSQL("dbo", "Test", changed, before, after, meta)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Redo WHERE = before PK (ID=1); Undo WHERE = after PK (ID=2).
	wantRedo := "update top(1) [dbo].[Test] set [ID]=2,[Val]=20 where [ID]=1;"
	wantUndo := "update top(1) [dbo].[Test] set [ID]=1,[Val]=10 where [ID]=2;"

	if redo != wantRedo {
		t.Errorf("redo:\n  got  %q\n  want %q", redo, wantRedo)
	}
	if undo != wantUndo {
		t.Errorf("undo:\n  got  %q\n  want %q", undo, wantUndo)
	}
}

func TestBuildUpdateSQL_NoChangedColumns(t *testing.T) {
	meta := makeTable("dbo", "T", []int{1}, fixedCol(1, "ID", schema.TypeInt))
	before := buildRow("ID", sv("ID", schema.TypeInt, 1))
	_, _, err := BuildUpdateSQL("dbo", "T", nil, before, before, meta)
	if err == nil {
		t.Error("expected error for empty changed columns list")
	}
}

func TestDiffRows_DetectsChange(t *testing.T) {
	meta := makeTable("dbo", "T", []int{1},
		fixedCol(1, "ID", schema.TypeInt),
		fixedCol(2, "Val", schema.TypeInt),
	)
	before := buildRow("ID", sv("ID", schema.TypeInt, 1), "Val", sv("Val", schema.TypeInt, 10))
	after := buildRow("ID", sv("ID", schema.TypeInt, 1), "Val", sv("Val", schema.TypeInt, 20))

	changed := DiffRows(before, after, meta)
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed column, got %d", len(changed))
	}
	if changed[0].ColumnName != "Val" {
		t.Errorf("expected Val changed, got %q", changed[0].ColumnName)
	}
}

func TestDiffRows_NullChange(t *testing.T) {
	meta := makeTable("dbo", "T", []int{1},
		fixedCol(1, "ID", schema.TypeInt),
		fixedCol(2, "Val", schema.TypeInt),
	)
	nullVal := SqlValue{ColumnName: "Val", TypeName: "int", IsNull: true, SQLLiteral: "NULL"}
	before := buildRow("ID", sv("ID", schema.TypeInt, 1), "Val", sv("Val", schema.TypeInt, 5))
	after := buildRow("ID", sv("ID", schema.TypeInt, 1), "Val", nullVal)

	changed := DiffRows(before, after, meta)
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed column, got %d", len(changed))
	}
}

func TestDiffRows_SkipsTimestamp(t *testing.T) {
	meta := &schema.Table{
		Schema: "dbo", Name: "T",
		PKCols: []int{1},
		Columns: []*schema.Column{
			{ColumnID: 1, Name: "ID", TypeID: schema.TypeInt, IsFixed: true},
			{ColumnID: 2, Name: "RowVer", TypeID: TypeTimestamp, IsFixed: true},
		},
	}
	before := buildRow("ID", sv("ID", schema.TypeInt, 1), "RowVer", sv("RowVer", TypeTimestamp, 100))
	after := buildRow("ID", sv("ID", schema.TypeInt, 1), "RowVer", sv("RowVer", TypeTimestamp, 200))

	changed := DiffRows(before, after, meta)
	if len(changed) != 0 {
		t.Errorf("timestamp column must be skipped, got %d changed", len(changed))
	}
}
