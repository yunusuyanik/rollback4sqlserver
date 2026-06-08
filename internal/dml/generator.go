// Package dml generates T-SQL DML statements from decoded log records.
package dml

import (
	"fmt"
	"strings"
	"time"

	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/rowdecoder"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// Statement is one generated DML statement with metadata.
type Statement struct {
	LSN           string
	TransactionID string
	Operation     string    // INSERT | UPDATE | DELETE
	Table         string
	SQL           string    // forward SQL — replays what happened
	RollbackSQL   string    // inverse SQL — undoes what happened
	Timestamp     time.Time // transaction begin time from LOP_BEGIN_XACT
	Database      string    // SQL Server database name
}

// Generator converts log records into DML statements.
type Generator struct {
	sch *schema.Schema
}

func New(sch *schema.Schema) *Generator {
	return &Generator{sch: sch}
}

// Generate decodes a log record and produces a DML statement.
// Returns nil, nil for non-data operations (BEGIN/COMMIT/etc.).
func (g *Generator) Generate(rec *logparser.LogRecord) (*Statement, error) {
	if !rec.IsDataOp() {
		return nil, nil
	}
	// NCI leaf/interior records contain index key tuples, not full row images.
	// Decoding them with the base-table schema produces corrupt SQL.
	if rec.Context != "LCX_HEAP" && rec.Context != "LCX_CLUSTERED" {
		return nil, nil
	}

	t := g.sch.Lookup(rec.AllocUnitName)
	if t == nil {
		return nil, nil // unknown table (system table, temp table, etc.)
	}

	tableName := fmt.Sprintf("[%s].[%s]", t.Schema, t.Name)

	switch rec.Operation {
	case logparser.OpInsertRows:
		return g.insert(rec, t, tableName)

	case logparser.OpDeleteRows:
		return g.delete(rec, t, tableName)

	case logparser.OpModifyRow:
		return g.update(rec, t, tableName)

	case logparser.OpModifyColumns:
		// Complex variable-length update — emit a comment for now.
		return &Statement{
			LSN:           rec.LSN,
			TransactionID: rec.TransactionID,
			Operation:     rec.Operation,
			Table:         tableName,
			SQL:           fmt.Sprintf("-- LOP_MODIFY_COLUMNS on %s (complex update, not decoded)", tableName),
		}, nil
	}

	return nil, nil
}

func (g *Generator) insert(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	vals, err := rowdecoder.DecodeRow(rec.Contents0, t)
	if err != nil {
		return nil, fmt.Errorf("INSERT decode: %w", err)
	}

	cols, placeholders := colsAndValues(t.Columns, vals)
	forwardSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, cols, placeholders)

	// Rollback: delete the row that was inserted, matched by PK.
	where, ok := buildWhere(t, vals)
	var rollbackSQL string
	if !ok {
		rollbackSQL = fmt.Sprintf("-- INSERT rollback unsafe: PK columns not resolvable for %s — do not execute", tableName)
	} else {
		rollbackSQL = fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, where)
	}

	return &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     "INSERT",
		Table:         tableName,
		SQL:           forwardSQL,
		RollbackSQL:   rollbackSQL,
	}, nil
}

func (g *Generator) delete(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	// Contents0 for LOP_DELETE_ROWS is the before-image (the deleted row).
	vals, err := rowdecoder.DecodeRow(rec.Contents0, t)
	if err != nil {
		return nil, fmt.Errorf("DELETE decode: %w", err)
	}

	where, ok := buildWhere(t, vals)
	var forwardSQL string
	if !ok {
		forwardSQL = fmt.Sprintf("-- DELETE unsafe: PK columns not resolvable for %s — do not execute", tableName)
	} else {
		forwardSQL = fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, where)
	}

	// Rollback: re-insert the deleted row with its original values.
	cols, placeholders := colsAndValues(t.Columns, vals)
	rollbackSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, cols, placeholders)

	return &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     "DELETE",
		Table:         tableName,
		SQL:           forwardSQL,
		RollbackSQL:   rollbackSQL,
	}, nil
}

func (g *Generator) update(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	// Contents0 = after-image; Contents1 = XOR delta to reconstruct before-image.
	afterVals, err := rowdecoder.DecodeRow(rec.Contents0, t)
	if err != nil {
		return nil, fmt.Errorf("UPDATE after decode: %w", err)
	}

	// LOP_MODIFY_ROW Contents0 is a positional delta (offset+len+bytes), not a full
	// row image — DecodeRow returns all-NULLs in this case. Emit a comment instead of
	// generating invalid SQL.
	if allNull(afterVals) {
		return &Statement{
			LSN:           rec.LSN,
			TransactionID: rec.TransactionID,
			Operation:     "UPDATE",
			Table:         tableName,
			SQL:           fmt.Sprintf("-- LOP_MODIFY_ROW on %s (partial log record — column values not recoverable from fn_dblog delta)", tableName),
			RollbackSQL:   fmt.Sprintf("-- LOP_MODIFY_ROW rollback on %s (not available — query current row by PK before running rollback)", tableName),
		}, nil
	}

	beforeData := rowdecoder.ReconstructBeforeImage(rec.Contents0, rec.Contents1)
	beforeVals, err := rowdecoder.DecodeRow(beforeData, t)
	if err != nil {
		beforeVals = afterVals // fallback: no before-image available
	}

	// Forward SQL: SET new values WHERE old PK.
	set := buildSet(t.Columns, afterVals)
	where, whereOK := buildWhere(t, beforeVals)
	var forwardSQL string
	if !whereOK {
		forwardSQL = fmt.Sprintf("-- UPDATE unsafe: PK columns not resolvable for %s — do not execute", tableName)
	} else {
		forwardSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, set, where)
	}

	// Rollback SQL: SET old values back WHERE current (after) PK.
	rollbackSet := buildSet(t.Columns, beforeVals)
	rollbackWhere, rollbackWhereOK := buildWhere(t, afterVals)
	var rollbackSQL string
	if !rollbackWhereOK {
		rollbackSQL = fmt.Sprintf("-- UPDATE rollback unsafe: PK columns not resolvable for %s — do not execute", tableName)
	} else {
		rollbackSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, rollbackSet, rollbackWhere)
	}

	return &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     "UPDATE",
		Table:         tableName,
		SQL:           forwardSQL,
		RollbackSQL:   rollbackSQL,
	}, nil
}

// allNull returns true when every value in the slice is nil or NULL.
func allNull(vals []*rowdecoder.Value) bool {
	for _, v := range vals {
		if v != nil && !v.IsNull {
			return false
		}
	}
	return true
}

// colsAndValues returns (column list, value list) for an INSERT.
func colsAndValues(cols []*schema.Column, vals []*rowdecoder.Value) (string, string) {
	var cnames, vstr []string
	for i, col := range cols {
		if i >= len(vals) {
			break
		}
		cnames = append(cnames, "["+col.Name+"]")
		vstr = append(vstr, formatValue(vals[i], col.TypeID))
	}
	return strings.Join(cnames, ", "), strings.Join(vstr, ", ")
}

// buildSet returns the SET clause for UPDATE.
func buildSet(cols []*schema.Column, vals []*rowdecoder.Value) string {
	var parts []string
	for i, col := range cols {
		if i >= len(vals) {
			break
		}
		parts = append(parts, fmt.Sprintf("[%s] = %s", col.Name, formatValue(vals[i], col.TypeID)))
	}
	return strings.Join(parts, ", ")
}

// buildWhere returns (whereClause, true) for DELETE / UPDATE.
// Returns ("", false) when no safe WHERE can be built — callers must emit a
// comment instead of executing the statement.
// Uses PK columns if available, otherwise all columns.
func buildWhere(t *schema.Table, vals []*rowdecoder.Value) (string, bool) {
	pkSet := make(map[int]bool, len(t.PKCols))
	for _, cid := range t.PKCols {
		pkSet[cid] = true
	}

	usePK := len(t.PKCols) > 0
	var parts []string
	for i, col := range t.Columns {
		if i >= len(vals) {
			break
		}
		if usePK && !pkSet[col.ColumnID] {
			continue
		}
		v := vals[i]
		if v.IsNull {
			parts = append(parts, fmt.Sprintf("[%s] IS NULL", col.Name))
		} else {
			parts = append(parts, fmt.Sprintf("[%s] = %s", col.Name, formatValue(v, col.TypeID)))
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, " AND "), true
}

// formatValue converts a decoded value to its T-SQL literal representation.
func formatValue(v *rowdecoder.Value, typeID int) string {
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
			return "N'" + escapeSQ(val) + "'"
		default:
			return "'" + escapeSQ(val) + "'"
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

func escapeSQ(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
