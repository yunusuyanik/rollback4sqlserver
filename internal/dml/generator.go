// Package dml generates T-SQL DML statements from decoded log records.
package dml

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/mssql"
	"github.com/uns/mssqllogrecovery/internal/rowdecoder"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// Statement is one generated DML statement with metadata.
type Statement struct {
	LSN           string
	TransactionID string
	Operation     string // INSERT | UPDATE | DELETE
	Table         string
	SQL           string    // forward SQL — replays what happened
	RollbackSQL   string    // inverse SQL — undoes what happened
	Timestamp     time.Time // transaction begin time from LOP_BEGIN_XACT
	CommitTime    time.Time // transaction commit time from LOP_COMMIT_XACT
	Database      string    // SQL Server database name

	// SkipReason is non-empty when SQL generation was intentionally suppressed.
	// SQL and RollbackSQL are comment strings (not executable) when this is set.
	SkipReason string `json:"skip_reason,omitempty"`

	Status                  string `json:"status,omitempty"`
	Confidence              string `json:"confidence,omitempty"`
	CompressionType         string `json:"compression_type,omitempty"`
	CompressedRowHex        string `json:"compressed_row_hex,omitempty"`
	DecompressedDebugJSON   string `json:"decompressed_debug_json,omitempty"`
	CompressionDecodeStatus string `json:"compression_decode_status,omitempty"`
}

// Generator converts log records into DML statements.
type Generator struct {
	sch               *schema.Schema
	compressedDecoder mssql.CompressedRowDecoder
	rowCache          *mssql.RowImageCache // INSERT row images keyed by page/slot; used for MODIFY_COLUMNS MR1
	pageReader        mssql.PageReader
}

func New(sch *schema.Schema) *Generator {
	return &Generator{
		sch:               sch,
		compressedDecoder: &mssql.MSSQLCompressedRowDecoder{},
		rowCache:          mssql.NewRowImageCache(),
		pageReader:        mssql.NopPageReader{},
	}
}

func NewWithPageReader(sch *schema.Schema, pageReader mssql.PageReader) *Generator {
	g := New(sch)
	if pageReader != nil {
		g.pageReader = pageReader
	}
	return g
}

// Generate decodes a log record and produces a DML statement.
// Returns nil, nil for non-data operations (BEGIN/COMMIT/etc.).
func (g *Generator) Generate(rec *logparser.LogRecord) (*Statement, error) {
	if !rec.IsDataOp() {
		return nil, nil
	}
	// NCI leaf/interior records contain index key tuples, not full row images.
	// Decoding them with the base-table schema produces corrupt SQL.
	// LCX_MARK_AS_GHOST is the context for clustered index row deletions (ghost-marking);
	// it carries the same full row before-image in Contents0 as LCX_CLUSTERED.
	if rec.Context != "LCX_HEAP" && rec.Context != "LCX_CLUSTERED" && rec.Context != "LCX_MARK_AS_GHOST" {
		return nil, nil
	}

	t := g.sch.LookupStorage(rec.AllocUnitName, rec.AllocUnitID, rec.PartitionID)
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
		return g.modifyColumns(rec, t, tableName)
	}

	return nil, nil
}

// logOpToOperation maps a raw fn_dblog operation name to a logical DML operation name.
func logOpToOperation(op string) string {
	switch op {
	case logparser.OpInsertRows:
		return "INSERT"
	case logparser.OpDeleteRows:
		return "DELETE"
	default: // LOP_MODIFY_ROW, LOP_MODIFY_COLUMNS
		return "UPDATE"
	}
}

// schemaMismatchSkip builds a Statement with SQL set to a comment and SkipReason = SCHEMA_MISMATCH.
// Emitted when the row's column count or column_id layout is inconsistent with the current schema,
// meaning the schema was changed (DROP COLUMN, or DROP+ADD) after this row was written.
// Re-running 'logrecovery schema' and re-scanning will resolve this once all rows are post-schema-change.
func schemaMismatchSkip(rec *logparser.LogRecord, tableName string) *Statement {
	msg := fmt.Sprintf("-- SCHEMA_MISMATCH: %s row layout does not match current schema — re-run 'logrecovery schema' to refresh", tableName)
	return &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     logOpToOperation(rec.Operation),
		Table:         tableName,
		SQL:           msg,
		RollbackSQL:   msg,
		SkipReason:    "SCHEMA_MISMATCH",
	}
}

// lobSkip builds a Statement with SQL set to a comment and SkipReason = OFF_ROW_LOB_NOT_SUPPORTED.
// SQL and RollbackSQL are not executable. The statement is emitted to preserve the audit trail
// (LSN, transaction ID, table, operation) even though the LOB values cannot be recovered.
func lobSkip(rec *logparser.LogRecord, tableName string) *Statement {
	msg := fmt.Sprintf("-- OFF_ROW_LOB_NOT_SUPPORTED: %s contains off-row LOB data not recoverable from the log row image", tableName)
	return &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     logOpToOperation(rec.Operation),
		Table:         tableName,
		SQL:           msg,
		RollbackSQL:   msg,
		SkipReason:    "OFF_ROW_LOB_NOT_SUPPORTED",
	}
}

func (g *Generator) insert(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	// Cache the raw row image so a later LOP_MODIFY_COLUMNS on the same slot can use it as MR1.
	if rec.PageID != "" && rec.SlotID != nil && len(rec.Contents0) > 0 {
		g.rowCache.Set(mssql.RowImageKey{
			PageID:      rec.PageID,
			SlotID:      *rec.SlotID,
			AllocUnitID: rec.AllocUnitID,
		}, rec.Contents0)
	}

	vals, compression, debug, stmt, err := g.decodeRowImage(rec, t, tableName)
	if err != nil {
		return nil, err
	}
	if stmt != nil {
		stmt.Operation = "INSERT"
		return stmt, nil
	}

	cols, placeholders := colsAndValues(t.Columns, vals)
	forwardSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, cols, placeholders)

	where, ok := buildWhere(t, vals)
	var rollbackSQL string
	if !ok {
		rollbackSQL = fmt.Sprintf("-- INSERT rollback unsafe: PK columns not resolvable for %s — do not execute", tableName)
	} else {
		rollbackSQL = fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, where)
	}

	out := &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     "INSERT",
		Table:         tableName,
		SQL:           forwardSQL,
		RollbackSQL:   rollbackSQL,
	}
	applyCompressionSuccess(out, compression, rec, debug)
	return out, nil
}

func (g *Generator) delete(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	defer g.deleteCachedRow(rec)
	vals, compression, debug, stmt, err := g.decodeRowImage(rec, t, tableName)
	if err != nil {
		return nil, err
	}
	if stmt != nil {
		stmt.Operation = "DELETE"
		return stmt, nil
	}

	// Sanity-check PK values before generating SQL.
	// Compressed rows decoded from a wrong payload offset produce garbage integer values.
	// Emitting those as WHERE clause is worse than emitting nothing.
	if invalidReason := validatePKValues(t, vals); invalidReason != "" {
		msg := fmt.Sprintf("-- compressed_key_decode_invalid: %s — %s", tableName, invalidReason)
		cols, placeholders := colsAndValues(t.Columns, vals)
		out := &Statement{
			LSN:                     rec.LSN,
			TransactionID:           rec.TransactionID,
			Operation:               "DELETE",
			Table:                   tableName,
			SQL:                     msg,
			RollbackSQL:             fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, cols, placeholders),
			SkipReason:              "compressed_key_decode_invalid",
			Status:                  "compressed_key_decode_invalid",
			Confidence:              "low",
			CompressionType:         string(compression),
			CompressedRowHex:        hex.EncodeToString(rec.Contents0),
			DecompressedDebugJSON:   mssql.MarshalCompressionDebug(debug),
			CompressionDecodeStatus: "compressed_key_decode_invalid",
		}
		return out, nil
	}

	where, ok := buildWhere(t, vals)
	var forwardSQL string
	if !ok {
		forwardSQL = fmt.Sprintf("-- DELETE unsafe: PK columns not resolvable for %s — do not execute", tableName)
	} else {
		forwardSQL = fmt.Sprintf("DELETE FROM %s WHERE %s;", tableName, where)
	}

	cols, placeholders := colsAndValues(t.Columns, vals)
	rollbackSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, cols, placeholders)

	out := &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     "DELETE",
		Table:         tableName,
		SQL:           forwardSQL,
		RollbackSQL:   rollbackSQL,
	}
	applyCompressionSuccess(out, compression, rec, debug)
	return out, nil
}

func (g *Generator) update(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	if beforeImage, afterImage, ok := g.reconstructModifyRow(rec); ok {
		return g.buildFullImageUpdate(rec, t, tableName, beforeImage, afterImage)
	}

	// Without a cached full row image, only fixed-column fragments can be decoded.
	if rec.OffsetInRow >= 0 {
		before, after := rowdecoder.DecodeModifyRowFragment(rec.Contents0, rec.Contents1, rec.OffsetInRow, t)
		return buildDeltaUpdate(rec, t, tableName, before, after, rec.OffsetInRow), nil
	}

	// Legacy fallback for log formats that carry a full after-image.
	afterVals, err := rowdecoder.DecodeRow(rec.Contents0, t)
	if err != nil {
		if errors.Is(err, rowdecoder.ErrOffRowLOB) {
			return lobSkip(rec, tableName), nil
		}
		if errors.Is(err, rowdecoder.ErrSchemaMismatch) {
			return schemaMismatchSkip(rec, tableName), nil
		}
		if errors.Is(err, rowdecoder.ErrCompressedRow) {
			return g.compressedPartialSkip(rec, tableName, t, err), nil
		}
		return decodeFallbackSkip(rec, tableName, err), nil
	}

	// LOP_MODIFY_ROW Contents0 is sometimes a positional delta (new fragment bytes),
	// not a full row image — DecodeRow returns all-NULLs in that case.
	// Recover partial column values from the changed region using the byte offset.
	if allNull(afterVals) {
		// Fallback: fingerprint the [Log Record] binary (SQL Server 2016+, no OffsetInRow).
		if delta := rowdecoder.ParseModifyRowDelta(rec.RawLogRecord, rec.Contents0, rec.Contents1, t); delta != nil {
			return buildDeltaUpdate(rec, t, tableName, delta.Before, delta.After, delta.RowOffset), nil
		}
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
		if errors.Is(err, rowdecoder.ErrOffRowLOB) || errors.Is(err, rowdecoder.ErrCompressedRow) {
			return lobSkip(rec, tableName), nil
		}
		if errors.Is(err, rowdecoder.ErrSchemaMismatch) {
			return schemaMismatchSkip(rec, tableName), nil
		}
		beforeVals = afterVals // fallback: no before-image available for other decode errors
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

func (g *Generator) reconstructModifyRow(rec *logparser.LogRecord) ([]byte, []byte, bool) {
	key, ok := rowImageKey(rec)
	if !ok || rec.OffsetInRow < 0 {
		return nil, nil, false
	}
	entry, ok := g.rowCache.Get(key)
	if !ok {
		return nil, nil, false
	}
	after, err := mssql.ApplyModifyRowFragment(entry.MR1, rec.Contents0, rec.Contents1, rec.OffsetInRow)
	if err != nil {
		return nil, nil, false
	}
	before := append([]byte(nil), entry.MR1...)
	g.rowCache.Set(key, after)
	return before, after, true
}

func (g *Generator) buildFullImageUpdate(
	rec *logparser.LogRecord,
	t *schema.Table,
	tableName string,
	beforeImage, afterImage []byte,
) (*Statement, error) {
	effectiveTable := g.tableWithPhysicalCols(t, rec.PartitionID)
	compression := mssql.ResolveCompression(g.sch, t, rec.AllocUnitID, rec.PartitionID)
	beforeRow, beforeErr := mssql.DecodeDMLRowImage(beforeImage, effectiveTable, compression, g.compressedDecoder)
	afterRow, afterErr := mssql.DecodeDMLRowImage(afterImage, effectiveTable, compression, g.compressedDecoder)
	if beforeErr != nil || afterErr != nil {
		err := beforeErr
		if err == nil {
			err = afterErr
		}
		return compressionPartialEvent(rec, tableName, t, compression,
			"update_full_image_decode_failed", "low", afterImage, afterRow.CompressionDebug, err), nil
	}

	set := buildChangedSet(t.Columns, beforeRow.Values, afterRow.Values)
	if set == "" {
		msg := fmt.Sprintf("-- UPDATE on %s reconstructed but no changed columns were decoded", tableName)
		return &Statement{LSN: rec.LSN, TransactionID: rec.TransactionID, Operation: "UPDATE", Table: tableName, SQL: msg, RollbackSQL: msg}, nil
	}
	where, whereOK := buildWhere(t, beforeRow.Values)
	rollbackWhere, rollbackOK := buildWhere(t, afterRow.Values)
	if !whereOK || !rollbackOK {
		msg := fmt.Sprintf("-- UPDATE unsafe: PK columns not resolvable for %s", tableName)
		return &Statement{LSN: rec.LSN, TransactionID: rec.TransactionID, Operation: "UPDATE", Table: tableName, SQL: msg, RollbackSQL: msg}, nil
	}

	out := &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     "UPDATE",
		Table:         tableName,
		SQL:           fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, set, where),
		RollbackSQL:   fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, buildChangedSet(t.Columns, afterRow.Values, beforeRow.Values), rollbackWhere),
	}
	applyCompressionSuccess(out, compression, rec, afterRow.CompressionDebug)
	out.CompressedRowHex = hex.EncodeToString(afterImage)
	return out, nil
}

func rowImageKey(rec *logparser.LogRecord) (mssql.RowImageKey, bool) {
	if rec.PageID == "" || rec.SlotID == nil {
		return mssql.RowImageKey{}, false
	}
	return mssql.RowImageKey{PageID: rec.PageID, SlotID: *rec.SlotID, AllocUnitID: rec.AllocUnitID}, true
}

func (g *Generator) deleteCachedRow(rec *logparser.LogRecord) {
	if key, ok := rowImageKey(rec); ok {
		g.rowCache.Delete(key)
	}
}

// modifyColumns handles LOP_MODIFY_COLUMNS (variable-length column updates).
// Tries cache lookup + reverse-patch to reconstruct before/after images.
// Falls back to a debug event with all rawlog hex when that fails.
func (g *Generator) modifyColumns(rec *logparser.LogRecord, t *schema.Table, tableName string) (*Statement, error) {
	// failReason accumulates why each MR1-resolution branch failed, so the
	// fallback debug event pinpoints the break instead of saying "not decoded".
	var failParts []string
	addFail := func(format string, a ...interface{}) { failParts = append(failParts, fmt.Sprintf(format, a...)) }
	var mr1Len int
	var mr1Source string

	if rec.PageID != "" && rec.SlotID != nil {
		key := mssql.RowImageKey{
			PageID:      rec.PageID,
			SlotID:      *rec.SlotID,
			AllocUnitID: rec.AllocUnitID,
		}
		if entry, ok := g.rowCache.Get(key); ok {
			mr1Source = "cache"
			mr1Len = len(entry.MR1)
			if after, patchErr := mssql.ApplyModifyColumns(entry.MR1, *rec); patchErr == nil {
				before := append([]byte(nil), entry.MR1...)
				g.rowCache.Set(key, after)
				return g.buildFullImageUpdate(rec, t, tableName, before, after)
			} else {
				addFail("cache.ApplyModifyColumns: %v", patchErr)
			}
			mr0, _, patchErr := mssql.BuildBeforeImageFromUpdateLog(*rec, entry.MR1, t)
			if patchErr != nil {
				addFail("cache.BuildBeforeImage: %v", patchErr)
			}
			if patchErr == nil {
				compression := mssql.ResolveCompression(g.sch, t, rec.AllocUnitID, rec.PartitionID)
				physTable := g.tableWithPhysicalCols(t, rec.PartitionID)
				afterRow, afterErr := mssql.DecodeDMLRowImage(entry.MR1, physTable, compression, g.compressedDecoder)
				beforeRow, beforeErr := mssql.DecodeDMLRowImage(mr0, physTable, compression, g.compressedDecoder)
				if afterErr != nil {
					addFail("cache.DecodeAfter: %v", afterErr)
				}
				if beforeErr != nil {
					addFail("cache.DecodeBefore: %v", beforeErr)
				}
				if afterErr == nil && beforeErr == nil {
					set := buildSet(t.Columns, afterRow.Values)
					where, whereOK := buildWhere(t, beforeRow.Values)
					if whereOK {
						rollbackSet := buildSet(t.Columns, beforeRow.Values)
						rollbackWhere, rollbackOK := buildWhere(t, afterRow.Values)
						var rollbackSQL string
						if rollbackOK {
							rollbackSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, rollbackSet, rollbackWhere)
						} else {
							rollbackSQL = fmt.Sprintf("-- UPDATE rollback unsafe: PK not resolvable for %s", tableName)
						}
						return &Statement{
							LSN:           rec.LSN,
							TransactionID: rec.TransactionID,
							Operation:     "UPDATE",
							Table:         tableName,
							SQL:           fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, set, where),
							RollbackSQL:   rollbackSQL,
						}, nil
					} else {
						addFail("cache.buildWhere: PK not resolvable")
					}
				}
			}
		} else {
			addFail("cache: miss (page/slot/allocunit not in row image cache)")
		}
	}
	mr1, mr1Src, err := mssql.ResolveMR1(context.Background(), *rec,
		mssql.ExtractPrimaryKeyProbeFromRowLog2(rec.Contents2), g.rowCache, g.pageReader)
	if err != nil {
		addFail("ResolveMR1: %v", err)
	} else {
		mr1Source = mr1Src
		mr1Len = len(mr1)
		if mr0, reverseErr := mssql.ReverseModifyColumns(mr1, *rec); reverseErr == nil {
			if key, ok := rowImageKey(rec); ok {
				g.rowCache.Set(key, mr1)
			}
			return g.buildFullImageUpdate(rec, t, tableName, mr0, mr1)
		} else {
			addFail("ReverseModifyColumns(mr1Len=%d,src=%s): %v", len(mr1), mr1Src, reverseErr)
		}
	}

	// Fallback: write event with raw hex so nothing is silently lost.
	slotStr := "null"
	if rec.SlotID != nil {
		slotStr = fmt.Sprintf("%d", *rec.SlotID)
	}
	failReason := strings.Join(failParts, " | ")
	debugJSON := fmt.Sprintf(
		`{"reason":"modify_columns_not_decoded","fail_reason":%q,"mr1_len":%d,"mr1_source":%q,"operation":"%s","context":"%s","rowlog0_hex":"%s","rowlog1_hex":"%s","rowlog2_hex":"%s","rowlog3_hex":"%s","rowlog4_hex":"%s","raw_log_record_hex":"%s","page_id":"%s","slot_id":%s,"alloc_unit_id":%d,"partition_id":%d,"offset_in_row":%d,"modify_size":%d}`,
		failReason,
		mr1Len,
		mr1Source,
		rec.Operation,
		rec.Context,
		hex.EncodeToString(rec.Contents0),
		hex.EncodeToString(rec.Contents1),
		hex.EncodeToString(rec.Contents2),
		hex.EncodeToString(rec.Contents3),
		hex.EncodeToString(rec.Contents4),
		hex.EncodeToString(rec.RawLogRecord),
		rec.PageID,
		slotStr,
		rec.AllocUnitID,
		rec.PartitionID,
		rec.OffsetInRow,
		rec.ModifySize,
	)
	msg := fmt.Sprintf("-- LOP_MODIFY_COLUMNS on %s (not decoded — see decompressed_debug_json for raw hex)", tableName)
	return &Statement{
		LSN:                   rec.LSN,
		TransactionID:         rec.TransactionID,
		Operation:             "UPDATE",
		Table:                 tableName,
		SQL:                   msg,
		RollbackSQL:           msg,
		SkipReason:            "modify_columns_not_decoded",
		Status:                "modify_columns_not_decoded",
		Confidence:            "low",
		DecompressedDebugJSON: debugJSON,
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

func buildChangedSet(cols []*schema.Column, before, after []*rowdecoder.Value) string {
	var parts []string
	for i, col := range cols {
		if i >= len(before) || i >= len(after) || valuesEqual(before[i], after[i]) {
			continue
		}
		parts = append(parts, fmt.Sprintf("[%s] = %s", col.Name, formatValue(after[i], col.TypeID)))
	}
	return strings.Join(parts, ", ")
}

func valuesEqual(a, b *rowdecoder.Value) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.IsNull || b.IsNull {
		return a.IsNull == b.IsNull
	}
	return fmt.Sprint(a.Raw) == fmt.Sprint(b.Raw)
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
		case schema.TypeNumeric, schema.TypeDecimal, schema.TypeMoney, schema.TypeSmallmoney:
			// Exact decimal string — no quotes, no float conversion.
			return val
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

// tableWithPhysicalCols returns a shallow copy of t with PhysicalColumns set from the
// partition-specific catalog when available.  Physical columns are required to get the
// correct nibble count for compressed rows that contain soft-dropped phantom columns.
func (g *Generator) tableWithPhysicalCols(t *schema.Table, partitionID int64) *schema.Table {
	if partitionID == 0 {
		return t
	}
	if physCols, ok := g.sch.PartitionPhysicalCols[partitionID]; ok && len(physCols) > 0 {
		tc := *t
		tc.PhysicalColumns = physCols
		return &tc
	}
	return t
}

// validatePKValues returns a non-empty reason string when the decoded primary-key
// values are clearly invalid (e.g. a non-nullable PK column decoded as NULL, or an
// INT value that overflows int32).  Callers must not emit DELETE SQL in that case.
func validatePKValues(t *schema.Table, vals []*rowdecoder.Value) string {
	if len(t.PKCols) == 0 {
		return "" // no PK known; caller may emit SQL
	}
	pkSet := make(map[int]bool, len(t.PKCols))
	for _, cid := range t.PKCols {
		pkSet[cid] = true
	}
	colByID := make(map[int]*schema.Column, len(t.Columns))
	for _, c := range t.Columns {
		colByID[c.ColumnID] = c
	}
	for i, col := range t.Columns {
		if !pkSet[col.ColumnID] {
			continue
		}
		if i >= len(vals) {
			return fmt.Sprintf("PK column %q missing from decoded values", col.Name)
		}
		v := vals[i]
		if v == nil || v.IsNull {
			if !col.IsNullable {
				return fmt.Sprintf("non-nullable PK column %q decoded as NULL", col.Name)
			}
			continue
		}
		// Type range guard: INT must fit in int32.
		if col.TypeID == schema.TypeInt {
			if n, ok := v.Raw.(int64); ok {
				if n < -2147483648 || n > 2147483647 {
					return fmt.Sprintf("PK column %q (int) decoded value %d out of int32 range", col.Name, n)
				}
			}
		}
		// Type range guard: SMALLINT must fit in int16.
		if col.TypeID == schema.TypeSmallint {
			if n, ok := v.Raw.(int64); ok {
				if n < -32768 || n > 32767 {
					return fmt.Sprintf("PK column %q (smallint) decoded value %d out of int16 range", col.Name, n)
				}
			}
		}
		// Type range guard: TINYINT must fit in 0–255.
		if col.TypeID == schema.TypeTinyint {
			if n, ok := v.Raw.(int64); ok {
				if n < 0 || n > 255 {
					return fmt.Sprintf("PK column %q (tinyint) decoded value %d out of range", col.Name, n)
				}
			}
		}
	}
	return ""
}

func (g *Generator) decodeRowImage(rec *logparser.LogRecord, t *schema.Table, tableName string) ([]*rowdecoder.Value, mssql.CompressionType, mssql.CompressionDebugInfo, *Statement, error) {
	// Use partition-specific physical columns when available.  This is the authoritative
	// source for the compressed row descriptor nibble count, which can differ from
	// sys.columns when the table has soft-dropped columns.
	effectiveTable := g.tableWithPhysicalCols(t, rec.PartitionID)

	compression := mssql.ResolveCompression(g.sch, t, rec.AllocUnitID, rec.PartitionID)
	if compression == mssql.CompressionUnknown && mssql.LooksCompressedRow(rec.Contents0) {
		return nil, compression, mssql.CompressionDebugInfo{}, compressionPartialEvent(rec, tableName, t, compression,
			"schema_compression_metadata_missing", "low", rec.Contents0, mssql.CompressionDebugInfo{}, mssql.ErrCompressionMetadataMissing), nil
	}
	decoded, err := mssql.DecodeDMLRowImage(rec.Contents0, effectiveTable, compression, g.compressedDecoder)

	// Compression metadata can be stale or wrong for a specific partition/alloc unit
	// (e.g. a partitioned table where this partition is uncompressed but the table
	// default is ROW/PAGE). A genuine ROW/PAGE row image is always CD-format with the
	// 0x20 marker bit set in byte 0. When the compressed decoder rejects the header and
	// the row lacks that marker, the row is not actually compressed — retry as an
	// uncompressed FixedVar record. The uncompressed decoder validates its own header
	// (Foffset bounds, column layout), so a wrong guess fails cleanly rather than
	// emitting corrupt SQL.
	if err != nil && errors.Is(err, mssql.ErrCompressedRowParseFailed) &&
		(compression == mssql.CompressionRow || compression == mssql.CompressionPage) &&
		len(rec.Contents0) > 0 && rec.Contents0[0]&0x20 == 0 {
		if uncompressed, uErr := mssql.DecodeDMLRowImage(rec.Contents0, t, mssql.CompressionNone, g.compressedDecoder); uErr == nil {
			return uncompressed.Values, mssql.CompressionNone, uncompressed.CompressionDebug, nil, nil
		}
	}

	if err != nil {
		switch {
		case errors.Is(err, rowdecoder.ErrOffRowLOB):
			return nil, compression, decoded.CompressionDebug, lobSkip(rec, tableName), nil
		case errors.Is(err, rowdecoder.ErrSchemaMismatch):
			return nil, compression, decoded.CompressionDebug, schemaMismatchSkip(rec, tableName), nil
		case errors.Is(err, mssql.ErrCompressionMetadataMissing):
			return nil, compression, decoded.CompressionDebug, compressionPartialEvent(rec, tableName, t, compression,
				"schema_compression_metadata_missing", "low", rec.Contents0, decoded.CompressionDebug, err), nil
		case errors.Is(err, mssql.ErrCompressedRowParseFailed):
			return nil, compression, decoded.CompressionDebug, compressionPartialEvent(rec, tableName, t, compression,
				"compressed_row_parse_failed", "low", rec.Contents0, decoded.CompressionDebug, err), nil
		case errors.Is(err, mssql.ErrCompressedTailParseFailed):
			return nil, compression, decoded.CompressionDebug, compressionPartialEvent(rec, tableName, t, compression,
				"compressed_tail_parse_failed", "low", rec.Contents0, decoded.CompressionDebug, err), nil
		case errors.Is(err, mssql.ErrCompressedTypeNotSupported):
			return nil, compression, decoded.CompressionDebug, compressionPartialEvent(rec, tableName, t, compression,
				"compressed_type_not_supported", "low", rec.Contents0, decoded.CompressionDebug, err), nil
		case errors.Is(err, mssql.ErrCompressedPageReference):
			return nil, compression, decoded.CompressionDebug, compressionPartialEvent(rec, tableName, t, compression,
				"page_dictionary_required", "low", rec.Contents0, decoded.CompressionDebug, err), nil
		case errors.Is(err, rowdecoder.ErrCompressedRow):
			return nil, compression, decoded.CompressionDebug, g.compressedPartialSkip(rec, tableName, t, err), nil
		default:
			return nil, compression, decoded.CompressionDebug, decodeFallbackSkip(rec, tableName, err), nil
		}
	}
	return decoded.Values, compression, decoded.CompressionDebug, nil, nil
}

func (g *Generator) compressedPartialSkip(rec *logparser.LogRecord, tableName string, t *schema.Table, decodeErr error) *Statement {
	compression := mssql.ResolveCompression(g.sch, t, rec.AllocUnitID, rec.PartitionID)
	if compression == mssql.CompressionNone || compression == mssql.CompressionUnknown {
		compression = mssql.CompressionRow
	}
	decoded, cerr := g.compressedDecoder.Decode(rec.Contents0, t, compression)
	status := "compressed_row_parse_failed"
	errOut := decodeErr
	if cerr != nil {
		errOut = cerr
		switch {
		case errors.Is(cerr, mssql.ErrCompressionMetadataMissing):
			status = "schema_compression_metadata_missing"
		case errors.Is(cerr, mssql.ErrCompressedTailParseFailed):
			status = "compressed_tail_parse_failed"
		case errors.Is(cerr, mssql.ErrCompressedTypeNotSupported):
			status = "compressed_type_not_supported"
		case errors.Is(cerr, mssql.ErrCompressedPageReference):
			status = "page_dictionary_required"
		}
	}
	return compressionPartialEvent(rec, tableName, t, compression, status, "low", rec.Contents0, decoded.CompressionDebug, errOut)
}

func compressionPartialEvent(
	rec *logparser.LogRecord,
	tableName string,
	t *schema.Table,
	compression mssql.CompressionType,
	status, confidence string,
	row []byte,
	debug mssql.CompressionDebugInfo,
	decodeErr error,
) *Statement {
	if compression == mssql.CompressionUnknown && t != nil {
		compression = mssql.CompressionFromInt(t.DataCompression)
	}
	enrichCompressionDebug(&debug, rec)
	msg := fmt.Sprintf("-- %s: %s — %v", status, tableName, decodeErr)
	return &Statement{
		LSN:                     rec.LSN,
		TransactionID:           rec.TransactionID,
		Operation:               logOpToOperation(rec.Operation),
		Table:                   tableName,
		SQL:                     msg,
		RollbackSQL:             msg,
		SkipReason:              status,
		Status:                  status,
		Confidence:              confidence,
		CompressionType:         string(compression),
		CompressedRowHex:        hex.EncodeToString(row),
		DecompressedDebugJSON:   mssql.MarshalCompressionDebug(debug),
		CompressionDecodeStatus: status,
	}
}

func enrichCompressionDebug(debug *mssql.CompressionDebugInfo, rec *logparser.LogRecord) {
	debug.Operation = rec.Operation
	debug.Context = rec.Context
	debug.PageID = rec.PageID
	debug.SlotID = rec.SlotID
	debug.AllocUnitID = rec.AllocUnitID
	debug.PartitionID = rec.PartitionID
	debug.OffsetInRow = rec.OffsetInRow
	debug.ModifySize = rec.ModifySize
	debug.RowLog0Hex = hex.EncodeToString(rec.Contents0)
	debug.RowLog1Hex = hex.EncodeToString(rec.Contents1)
	debug.RowLog2Hex = hex.EncodeToString(rec.Contents2)
	debug.RowLog3Hex = hex.EncodeToString(rec.Contents3)
	debug.RowLog4Hex = hex.EncodeToString(rec.Contents4)
	debug.RawLogRecordHex = hex.EncodeToString(rec.RawLogRecord)
}

func applyCompressionSuccess(stmt *Statement, compression mssql.CompressionType, rec *logparser.LogRecord, debug mssql.CompressionDebugInfo) {
	if compression == mssql.CompressionRow || compression == mssql.CompressionPage {
		stmt.Status = "ok"
		stmt.Confidence = "high"
		stmt.CompressionType = string(compression)
		stmt.CompressedRowHex = hex.EncodeToString(rec.Contents0)
		stmt.DecompressedDebugJSON = mssql.MarshalCompressionDebug(debug)
		stmt.CompressionDecodeStatus = "ok"
	}
}

// decodeFallbackSkip emits a visible placeholder when DecodeRow fails with an error
// not covered by the specific skip helpers. Prevents silent event loss.
func decodeFallbackSkip(rec *logparser.LogRecord, tableName string, decodeErr error) *Statement {
	msg := fmt.Sprintf("-- DECODE_ERROR: %s — %v", tableName, decodeErr)
	return &Statement{
		LSN:           rec.LSN,
		TransactionID: rec.TransactionID,
		Operation:     logOpToOperation(rec.Operation),
		Table:         tableName,
		SQL:           msg,
		RollbackSQL:   msg,
		SkipReason:    "DECODE_ERROR",
	}
}

// buildDeltaUpdate generates partial UPDATE SQL from a LOP_MODIFY_ROW in-place delta.
// before[i]/after[i] are indexed by t.Columns; nil means the column was not in the delta.
// WHERE uses PK columns when they appear in the delta; otherwise falls back to changed
// columns with a safety note (may not be unique).
func buildDeltaUpdate(rec *logparser.LogRecord, t *schema.Table, tableName string, before, after []*rowdecoder.Value, offsetInRow int) *Statement {
	pkSet := make(map[int]bool, len(t.PKCols))
	for _, cid := range t.PKCols {
		pkSet[cid] = true
	}

	type colResult struct {
		name   string
		typeID int
		before *rowdecoder.Value
		after  *rowdecoder.Value
		isPK   bool
	}
	var changed []colResult
	for i, col := range t.Columns {
		if before[i] == nil && after[i] == nil {
			continue
		}
		changed = append(changed, colResult{
			name:   col.Name,
			typeID: col.TypeID,
			before: before[i],
			after:  after[i],
			isPK:   pkSet[col.ColumnID],
		})
	}

	if len(changed) == 0 {
		msg := fmt.Sprintf("-- LOP_MODIFY_ROW on %s (offset=%d): delta parsed but no column values decoded", tableName, offsetInRow)
		return &Statement{
			LSN: rec.LSN, TransactionID: rec.TransactionID, Operation: "UPDATE", Table: tableName,
			SQL: msg, RollbackSQL: msg,
		}
	}

	// Build SET and WHERE parts.
	var setFwd, setRbk []string
	var whereFwd, whereRbk []string
	var pkInDelta bool

	for _, c := range changed {
		colRef := "[" + c.name + "]"
		newVal := "NULL"
		if c.after != nil {
			newVal = formatValue(c.after, c.typeID)
		}
		oldVal := "NULL"
		if c.before != nil {
			oldVal = formatValue(c.before, c.typeID)
		}

		setFwd = append(setFwd, fmt.Sprintf("%s = %s", colRef, newVal))
		setRbk = append(setRbk, fmt.Sprintf("%s = %s", colRef, oldVal))

		if c.isPK {
			pkInDelta = true
			if c.before != nil {
				whereFwd = append(whereFwd, fmt.Sprintf("%s = %s", colRef, oldVal))
			}
			if c.after != nil {
				whereRbk = append(whereRbk, fmt.Sprintf("%s = %s", colRef, newVal))
			}
		}
	}

	setFwdStr := strings.Join(setFwd, ", ")
	setRbkStr := strings.Join(setRbk, ", ")

	var forwardSQL, rollbackSQL string

	if pkInDelta && len(whereFwd) > 0 {
		forwardSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, setFwdStr, strings.Join(whereFwd, " AND "))
		if len(whereRbk) > 0 {
			rollbackSQL = fmt.Sprintf("UPDATE %s SET %s WHERE %s;", tableName, setRbkStr, strings.Join(whereRbk, " AND "))
		} else {
			rollbackSQL = fmt.Sprintf("-- UPDATE rollback on %s: PK after-value missing from delta", tableName)
		}
	} else {
		// PK not in delta — use changed columns in WHERE (may not be unique)
		var whereFromBefore []string
		for _, c := range changed {
			if c.before != nil {
				whereFromBefore = append(whereFromBefore, fmt.Sprintf("[%s] = %s", c.name, formatValue(c.before, c.typeID)))
			}
		}
		var whereFromAfter []string
		for _, c := range changed {
			if c.after != nil {
				whereFromAfter = append(whereFromAfter, fmt.Sprintf("[%s] = %s", c.name, formatValue(c.after, c.typeID)))
			}
		}

		safetyNote := fmt.Sprintf("-- PK not in log delta (offset=%d) — WHERE uses changed columns only; verify uniqueness before executing", offsetInRow)
		if len(whereFromBefore) > 0 {
			forwardSQL = fmt.Sprintf("%s\nUPDATE %s SET %s WHERE %s;", safetyNote, tableName, setFwdStr, strings.Join(whereFromBefore, " AND "))
		} else {
			forwardSQL = fmt.Sprintf("%s\n-- SET: UPDATE %s SET %s WHERE [pk] = ?;", safetyNote, tableName, setFwdStr)
		}
		if len(whereFromAfter) > 0 {
			rollbackSQL = fmt.Sprintf("%s\nUPDATE %s SET %s WHERE %s;", safetyNote, tableName, setRbkStr, strings.Join(whereFromAfter, " AND "))
		} else {
			rollbackSQL = fmt.Sprintf("%s\n-- SET (rollback): UPDATE %s SET %s WHERE [pk] = ?;", safetyNote, tableName, setRbkStr)
		}
	}

	return &Statement{
		LSN: rec.LSN, TransactionID: rec.TransactionID, Operation: "UPDATE", Table: tableName,
		SQL: forwardSQL, RollbackSQL: rollbackSQL,
	}
}
