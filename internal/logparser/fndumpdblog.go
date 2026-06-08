package logparser

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/microsoft/go-mssqldb"
)

const maxFilesPerCall = 64

// TRNReader reads log records from one or more TRN backup files using
// fn_dump_dblog on the given SQL Server connection.
// The connection can be the SOURCE server (fn_dump_dblog only reads the
// backup file from disk — no DML/DDL, no buffer pool pressure) or a
// separate analysis instance.
type TRNReader struct {
	db        *sql.DB
	files     []string // absolute paths on the SQL Server host
	startLSN  string   // "" = from beginning
	endLSN    string   // "" = to end
	chunkSize int      // max LSN records per call; 0 = unlimited
}

// NewTRNReader constructs a reader. files must be absolute paths accessible
// from the SQL Server process (not the Go client).
func NewTRNReader(db *sql.DB, files []string) *TRNReader {
	return &TRNReader{db: db, files: files}
}

// WithLSNRange restricts the scan to [startLSN, endLSN] (inclusive).
// Pass "" to leave a bound open.
func (r *TRNReader) WithLSNRange(startLSN, endLSN string) *TRNReader {
	r.startLSN = startLSN
	r.endLSN = endLSN
	return r
}

// Read streams all log records matching the operation filter.
// If ops is empty, all records are returned.
// The handler fn is called for each record; returning an error stops iteration.
func (r *TRNReader) Read(ops []string, fn func(*LogRecord) error) error {
	// Process at most maxFilesPerCall files per fn_dump_dblog call.
	for i := 0; i < len(r.files); i += maxFilesPerCall {
		end := i + maxFilesPerCall
		if end > len(r.files) {
			end = len(r.files)
		}
		if err := r.readBatch(r.files[i:end], ops, fn); err != nil {
			return err
		}
	}
	return nil
}

func (r *TRNReader) readBatch(files []string, ops []string, fn func(*LogRecord) error) error {
	query := buildDumpQuery(r.startLSN, r.endLSN, files, ops)

	rows, err := r.db.Query(query)
	if err != nil {
		return fmt.Errorf("fn_dump_dblog: %w", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return err
	}
	idx := columnIndex(colNames)

	for rows.Next() {
		rec, err := scanRecord(rows, colNames, idx)
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
	return rows.Err()
}

// buildDumpQuery constructs the fn_dump_dblog SELECT statement.
// File paths are embedded as string literals (single-quote escaped).
// An optional WHERE clause filters by operation type.
func buildDumpQuery(startLSN, endLSN string, files []string, ops []string) string {
	var sb strings.Builder

	sb.WriteString("SELECT [Current LSN],[Operation],[Context],[Transaction ID],")
	sb.WriteString("[AllocUnitName],[Begin time],[End time],")
	sb.WriteString("[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],")
	sb.WriteString("[RowLog Contents 3],[RowLog Contents 4],[Log Record]")
	sb.WriteString(" FROM fn_dump_dblog(")

	// param 1: start LSN
	if startLSN == "" {
		sb.WriteString("NULL")
	} else {
		fmt.Fprintf(&sb, "N'%s'", escapeSQ(startLSN))
	}
	sb.WriteString(",")

	// param 2: end LSN
	if endLSN == "" {
		sb.WriteString("NULL")
	} else {
		fmt.Fprintf(&sb, "N'%s'", escapeSQ(endLSN))
	}

	// param 3: device type
	sb.WriteString(",N'DISK'")

	// param 4: number of files
	fmt.Fprintf(&sb, ",%d", len(files))

	// params 5..5+N-1: file paths
	for _, f := range files {
		fmt.Fprintf(&sb, ",N'%s'", escapeSQ(f))
	}

	// remaining slots up to 64 must be DEFAULT
	for i := len(files); i < maxFilesPerCall; i++ {
		sb.WriteString(",DEFAULT")
	}

	sb.WriteString(")")

	if len(ops) > 0 {
		quoted := make([]string, len(ops))
		for i, op := range ops {
			quoted[i] = "'" + escapeSQ(op) + "'"
		}
		fmt.Fprintf(&sb, " WHERE [Operation] IN (%s)", strings.Join(quoted, ","))
	}

	return sb.String()
}

// escapeSQ escapes single quotes for embedding in an SQL string literal.
func escapeSQ(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// columnIndex builds a name→index map from rows.Columns().
func columnIndex(names []string) map[string]int {
	m := make(map[string]int, len(names))
	for i, n := range names {
		m[n] = i
	}
	return m
}

// scanRecord reads one row from fn_dump_dblog / fn_dblog into a LogRecord.
// It uses a dynamic column map so it works across SQL Server versions.
func scanRecord(rows *sql.Rows, colNames []string, idx map[string]int) (*LogRecord, error) {
	vals := make([]interface{}, len(colNames))
	for i := range vals {
		vals[i] = new(interface{})
	}
	if err := rows.Scan(vals...); err != nil {
		return nil, err
	}

	get := func(name string) interface{} {
		i, ok := idx[name]
		if !ok {
			return nil
		}
		return *(vals[i].(*interface{}))
	}
	str := func(name string) string {
		v := get(name)
		if v == nil {
			return ""
		}
		switch s := v.(type) {
		case string:
			return s
		case []byte:
			return string(s)
		}
		return fmt.Sprintf("%v", v)
	}
	blob := func(name string) []byte {
		v := get(name)
		if v == nil {
			return nil
		}
		b, ok := v.([]byte)
		if !ok {
			return nil
		}
		return b
	}

	return &LogRecord{
		LSN:           str("Current LSN"),
		Operation:     str("Operation"),
		Context:       str("Context"),
		TransactionID: str("Transaction ID"),
		AllocUnitName: str("AllocUnitName"),
		BeginTime:     str("Begin time"),
		EndTime:       str("End time"),
		Contents0:     blob("RowLog Contents 0"),
		Contents1:     blob("RowLog Contents 1"),
		Contents2:     blob("RowLog Contents 2"),
		Contents3:     blob("RowLog Contents 3"),
		Contents4:     blob("RowLog Contents 4"),
		RawLogRecord:  blob("Log Record"),
	}, nil
}
