package mssql

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/uns/mssqllogrecovery/internal/rowdecoder"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// TypeTimestamp is SQL Server system_type_id 189 (rowversion / timestamp).
// Excluded from diff and WHERE predicates.
const TypeTimestamp = 189

// SqlValue is a decoded column value with all representations needed for SQL
// generation and JSON export.
type SqlValue struct {
	ColumnName string
	TypeName   string
	IsNull     bool
	Raw        []byte // copy of []byte go values; nil for non-byte types
	GoValue    any
	SQLLiteral string
}

// UpdateRow maps column name → SqlValue.
type UpdateRow struct {
	Values map[string]SqlValue
}

// RowDecoder decodes a raw row image into typed values.
// UpdateDecoder decodes a raw row image into typed values.
type UpdateDecoder interface {
	Decode(row []byte, meta *schema.Table) (UpdateRow, error)
}

// StandardRowDecoder wraps rowdecoder.DecodeRow and converts its output to
// UpdateRow. Use this when the pipeline already uses the existing decoder.
type StandardRowDecoder struct{}

func (StandardRowDecoder) Decode(row []byte, meta *schema.Table) (UpdateRow, error) {
	vals, err := rowdecoder.DecodeRow(row, meta)
	if err != nil {
		return UpdateRow{}, err
	}
	result := UpdateRow{Values: make(map[string]SqlValue, len(meta.Columns))}
	for i, col := range meta.Columns {
		if i >= len(vals) {
			break
		}
		result.Values[col.Name] = toSqlValue(col, vals[i])
	}
	return result, nil
}

func toSqlValue(col *schema.Column, v *rowdecoder.Value) SqlValue {
	sv := SqlValue{
		ColumnName: col.Name,
		TypeName:   sqlTypeName(col),
	}
	if v == nil || v.IsNull {
		sv.IsNull = true
		sv.SQLLiteral = "NULL"
		return sv
	}
	sv.GoValue = v.Raw
	sv.SQLLiteral = formatSQLLiteral(v, col.TypeID)
	if b, ok := v.Raw.([]byte); ok {
		cp := make([]byte, len(b))
		copy(cp, b)
		sv.Raw = cp
	}
	return sv
}

// ChangedColumn describes one column that differed between before and after.
type ChangedColumn struct {
	ColumnName string
	TypeName   string
	Before     SqlValue
	After      SqlValue
}

// DiffRows returns columns whose value changed between before and after.
// Skips TypeTimestamp (rowversion) columns.
// Uses type-aware equality: canonical decimal strings, UTC time comparison,
// byte-slice equality for binary types.
func DiffRows(before, after UpdateRow, meta *schema.Table) []ChangedColumn {
	var changed []ChangedColumn
	for _, col := range meta.Columns {
		if col.TypeID == TypeTimestamp {
			continue
		}
		bv, bOK := before.Values[col.Name]
		av, aOK := after.Values[col.Name]
		if !bOK || !aOK {
			continue
		}
		if sqlValuesEqual(col, bv, av) {
			continue
		}
		changed = append(changed, ChangedColumn{
			ColumnName: col.Name,
			TypeName:   sqlTypeName(col),
			Before:     bv,
			After:      av,
		})
	}
	return changed
}

func sqlValuesEqual(col *schema.Column, a, b SqlValue) bool {
	if a.IsNull != b.IsNull {
		return false
	}
	if a.IsNull {
		return true // both null
	}
	switch col.TypeID {
	case schema.TypeNumeric, schema.TypeDecimal, schema.TypeMoney, schema.TypeSmallmoney:
		as, aok := a.GoValue.(string)
		bs, bok := b.GoValue.(string)
		if aok && bok {
			return canonicalDecimal(as) == canonicalDecimal(bs)
		}
	case schema.TypeDatetime, schema.TypeDatetime2, schema.TypeDatetimeoffset,
		schema.TypeDate, schema.TypeTime, schema.TypeSmalldatetime:
		at, aok := a.GoValue.(time.Time)
		bt, bok := b.GoValue.(time.Time)
		if aok && bok {
			return at.UTC().Equal(bt.UTC())
		}
	case schema.TypeVarbinary, schema.TypeBinary, schema.TypeImage:
		ab, aok := a.GoValue.([]byte)
		bb, bok := b.GoValue.([]byte)
		if aok && bok {
			return reflect.DeepEqual(ab, bb)
		}
	}
	return reflect.DeepEqual(a.GoValue, b.GoValue)
}

func canonicalDecimal(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
}

// BuildWherePredicate builds a WHERE clause string and returns the strategy
// used: "primary_key", or "full_row".
// Returns an error when no usable columns are found.
func BuildWherePredicate(row UpdateRow, meta *schema.Table) (clause string, strategy string, err error) {
	pkSet := make(map[int]bool, len(meta.PKCols))
	for _, cid := range meta.PKCols {
		pkSet[cid] = true
	}

	var cols []*schema.Column
	strategy = "primary_key"
	if len(meta.PKCols) > 0 {
		for _, col := range meta.Columns {
			if pkSet[col.ColumnID] {
				cols = append(cols, col)
			}
		}
	}

	// Fall through to full-row if PK not defined or no PK columns found.
	if len(cols) == 0 {
		strategy = "full_row"
		for _, col := range meta.Columns {
			if col.TypeID == TypeTimestamp || isLOBType(col) {
				continue
			}
			cols = append(cols, col)
		}
	}

	if len(cols) == 0 {
		return "", strategy, errors.New("no columns available for WHERE predicate")
	}

	var parts []string
	for _, col := range cols {
		sv, ok := row.Values[col.Name]
		if !ok {
			continue
		}
		if sv.IsNull {
			parts = append(parts, fmt.Sprintf("[%s] IS NULL", col.Name))
		} else {
			parts = append(parts, fmt.Sprintf("[%s]=%s", col.Name, sv.SQLLiteral))
		}
	}
	if len(parts) == 0 {
		return "", strategy, errors.New("WHERE predicate: no resolvable column values")
	}
	return strings.Join(parts, " AND "), strategy, nil
}

func isLOBType(col *schema.Column) bool {
	switch col.TypeID {
	case schema.TypeText, schema.TypeNtext, schema.TypeImage, schema.TypeXML:
		return true
	}
	return col.MaxLength < 0 // varchar(max), nvarchar(max), varbinary(max)
}

// BuildUpdateSQL generates (redo, undo) equivalent SQL from changed columns.
//
// Redo: SET after-values WHERE before-PK (or after-PK when PK unchanged).
// Undo: SET before-values WHERE after-PK (or before-PK when PK unchanged).
// If PK changed: redo WHERE = before PK, undo WHERE = after PK.
func BuildUpdateSQL(schemaName, table string, changed []ChangedColumn, before, after UpdateRow, meta *schema.Table) (redo, undo string, err error) {
	if len(changed) == 0 {
		return "", "", errors.New("no changed columns")
	}

	tableName := fmt.Sprintf("[%s].[%s]", schemaName, table)

	pkSet := make(map[int]bool, len(meta.PKCols))
	for _, cid := range meta.PKCols {
		pkSet[cid] = true
	}
	pkChanged := false
	for _, cc := range changed {
		for _, col := range meta.Columns {
			if col.Name == cc.ColumnName && pkSet[col.ColumnID] {
				pkChanged = true
				break
			}
		}
		if pkChanged {
			break
		}
	}

	var redoSet, undoSet []string
	for _, cc := range changed {
		redoSet = append(redoSet, fmt.Sprintf("[%s]=%s", cc.ColumnName, cc.After.SQLLiteral))
		undoSet = append(undoSet, fmt.Sprintf("[%s]=%s", cc.ColumnName, cc.Before.SQLLiteral))
	}

	// Redo WHERE: before PK (when PK changed, before PK is the old key to match).
	// When PK unchanged, before == after for PK, so before is fine either way.
	redoWhereRow := before
	undoWhereRow := after
	if pkChanged {
		redoWhereRow = before
		undoWhereRow = after
	}

	redoWhere, _, werr := BuildWherePredicate(redoWhereRow, meta)
	if werr != nil {
		return "", "", fmt.Errorf("redo WHERE: %w", werr)
	}
	undoWhere, _, werr := BuildWherePredicate(undoWhereRow, meta)
	if werr != nil {
		return "", "", fmt.Errorf("undo WHERE: %w", werr)
	}

	redo = fmt.Sprintf("update top(1) %s set %s where %s;",
		tableName, strings.Join(redoSet, ","), redoWhere)
	undo = fmt.Sprintf("update top(1) %s set %s where %s;",
		tableName, strings.Join(undoSet, ","), undoWhere)
	return redo, undo, nil
}

// formatSQLLiteral converts a decoded value to a T-SQL literal string.
func formatSQLLiteral(v *rowdecoder.Value, typeID int) string {
	if v == nil || v.IsNull {
		return "NULL"
	}
	switch val := v.Raw.(type) {
	case bool:
		if val {
			return "1"
		}
		return "0"
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case string:
		switch typeID {
		case schema.TypeNvarchar, schema.TypeNchar, schema.TypeNtext:
			return "N'" + escapeSQLStr(val) + "'"
		case schema.TypeNumeric, schema.TypeDecimal, schema.TypeMoney, schema.TypeSmallmoney:
			return val // exact decimal string, no quotes
		default:
			return "'" + escapeSQLStr(val) + "'"
		}
	case []byte:
		return fmt.Sprintf("0x%X", val)
	case time.Time:
		switch typeID {
		case schema.TypeDate:
			return "'" + val.Format("2006-01-02") + "'"
		case schema.TypeDatetime, schema.TypeSmalldatetime:
			return "'" + val.Format("2006-01-02 15:04:05.000") + "'"
		case schema.TypeDatetime2:
			return "'" + val.Format("2006-01-02 15:04:05.0000000") + "'"
		case schema.TypeDatetimeoffset:
			return "'" + val.Format("2006-01-02 15:04:05.0000000 -07:00") + "'"
		case schema.TypeTime:
			return "'" + val.Format("15:04:05.0000000") + "'"
		}
		return "'" + val.Format(time.RFC3339Nano) + "'"
	}
	return fmt.Sprintf("'%v'", v.Raw)
}

func escapeSQLStr(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func sqlTypeName(col *schema.Column) string {
	switch col.TypeID {
	case schema.TypeTinyint:
		return "tinyint"
	case schema.TypeSmallint:
		return "smallint"
	case schema.TypeInt:
		return "int"
	case schema.TypeBigint:
		return "bigint"
	case schema.TypeBit:
		return "bit"
	case schema.TypeReal:
		return "real"
	case schema.TypeFloat:
		return "float"
	case schema.TypeSmallmoney:
		return "smallmoney"
	case schema.TypeMoney:
		return "money"
	case schema.TypeSmalldatetime:
		return "smalldatetime"
	case schema.TypeDatetime:
		return "datetime"
	case schema.TypeDate:
		return "date"
	case schema.TypeTime:
		return fmt.Sprintf("time(%d)", col.Scale)
	case schema.TypeDatetime2:
		return fmt.Sprintf("datetime2(%d)", col.Scale)
	case schema.TypeDatetimeoffset:
		return fmt.Sprintf("datetimeoffset(%d)", col.Scale)
	case schema.TypeUniqueidentifier:
		return "uniqueidentifier"
	case schema.TypeChar:
		return fmt.Sprintf("char(%d)", col.MaxLength)
	case schema.TypeNchar:
		return fmt.Sprintf("nchar(%d)", col.MaxLength/2)
	case schema.TypeBinary:
		return fmt.Sprintf("binary(%d)", col.MaxLength)
	case schema.TypeNumeric:
		return fmt.Sprintf("numeric(%d,%d)", col.Precision, col.Scale)
	case schema.TypeDecimal:
		return fmt.Sprintf("decimal(%d,%d)", col.Precision, col.Scale)
	case schema.TypeVarchar:
		if col.MaxLength < 0 {
			return "varchar(max)"
		}
		return fmt.Sprintf("varchar(%d)", col.MaxLength)
	case schema.TypeNvarchar:
		if col.MaxLength < 0 {
			return "nvarchar(max)"
		}
		return fmt.Sprintf("nvarchar(%d)", col.MaxLength/2)
	case schema.TypeVarbinary:
		if col.MaxLength < 0 {
			return "varbinary(max)"
		}
		return fmt.Sprintf("varbinary(%d)", col.MaxLength)
	case schema.TypeText:
		return "text"
	case schema.TypeNtext:
		return "ntext"
	case schema.TypeImage:
		return "image"
	case schema.TypeXML:
		return "xml"
	case TypeTimestamp:
		return "timestamp"
	}
	return fmt.Sprintf("type_%d", col.TypeID)
}
