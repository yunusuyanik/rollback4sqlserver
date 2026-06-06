package logparser

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

// LDFReader reads live transaction log records from fn_dblog on a running
// SQL Server instance. It uses read-only SELECT — no DML/DDL.
//
// Load impact: fn_dblog reads from the in-memory log buffer + log file.
// Use WithDelay to throttle between chunks.
type LDFReader struct {
	db       *sql.DB
	startLSN string
	endLSN   string
	delay    time.Duration // sleep between LSN chunks (0 = no throttle)
}

// NewLDFReader constructs a reader against the given connection.
func NewLDFReader(db *sql.DB) *LDFReader {
	return &LDFReader{db: db}
}

// WithLSNRange restricts the scan.
func (r *LDFReader) WithLSNRange(startLSN, endLSN string) *LDFReader {
	r.startLSN = startLSN
	r.endLSN = endLSN
	return r
}

// WithDelay adds a sleep between calls to reduce server load.
func (r *LDFReader) WithDelay(d time.Duration) *LDFReader {
	r.delay = d
	return r
}

// Read streams log records. Same semantics as TRNReader.Read.
func (r *LDFReader) Read(ops []string, fn func(*LogRecord) error) error {
	query := buildLiveQuery(r.startLSN, r.endLSN, ops)

	rows, err := r.db.Query(query)
	if err != nil {
		return fmt.Errorf("fn_dblog: %w", err)
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
		if r.delay > 0 {
			time.Sleep(r.delay)
		}
	}
	return rows.Err()
}

func buildLiveQuery(startLSN, endLSN string, ops []string) string {
	var sb strings.Builder

	sb.WriteString("SELECT [Current LSN],[Operation],[Context],[Transaction ID],")
	sb.WriteString("[AllocUnitName],[Begin time],[End time],")
	sb.WriteString("[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],")
	sb.WriteString("[RowLog Contents 3],[RowLog Contents 4]")
	sb.WriteString(" FROM fn_dblog(")

	if startLSN == "" {
		sb.WriteString("NULL")
	} else {
		fmt.Fprintf(&sb, "N'%s'", escapeSQ(startLSN))
	}
	sb.WriteString(",")
	if endLSN == "" {
		sb.WriteString("NULL")
	} else {
		fmt.Fprintf(&sb, "N'%s'", escapeSQ(endLSN))
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
