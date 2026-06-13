package logparser

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/microsoft/go-mssqldb"
)

// LDFReader reads live transaction log records from fn_dblog on a running
// SQL Server instance. Always calls fn_dblog(NULL,NULL) filtered by
// WHERE [Current LSN] > startLSN. Read-only — no DML/DDL.
type LDFReader struct {
	db       *sql.DB
	startLSN string // exclusive lower bound ("" = from active-log head)
}

// NewLDFReader constructs a reader against the given connection.
func NewLDFReader(db *sql.DB) *LDFReader {
	return &LDFReader{db: db}
}

// WithStartLSN sets an exclusive lower bound. Records with [Current LSN] <= lsn
// are not returned. Use "" to read from the beginning of the active log.
func (r *LDFReader) WithStartLSN(lsn string) *LDFReader {
	r.startLSN = lsn
	return r
}

// Read streams relevant log records from fn_dblog(NULL,NULL) in natural log order.
// The handler fn is called for each record; returning an error stops iteration.
func (r *LDFReader) Read(fn func(*LogRecord) error) error {
	rows, err := r.db.Query(buildLiveQuery(r.startLSN))
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
	}
	return rows.Err()
}

// buildLiveQuery builds the fn_dblog(NULL,NULL) SELECT with a compound WHERE:
//   - Control records (LOP_BEGIN_XACT / LOP_COMMIT_XACT / LOP_ABORT_XACT):
//     no context or allocation-unit restriction.
//   - Data records (INSERT / DELETE / UPDATE / MODIFY_COLUMNS):
//     restricted to LCX_HEAP and LCX_CLUSTERED, with sys.* and
//     "Unknown Alloc Unit" names excluded at SQL level.
//
// afterLSN is an exclusive lower bound (empty = read from active-log head).
func buildLiveQuery(afterLSN string) string {
	var sb strings.Builder
	sb.WriteString("SELECT [Current LSN],[Operation],[Context],[Transaction ID],[Transaction Name],")
	sb.WriteString("[AllocUnitName],[AllocUnitId],[PartitionId],[Page ID],[Slot ID],[Begin time],[End time],")
	sb.WriteString("[RowLog Contents 0],[RowLog Contents 1],[RowLog Contents 2],")
	sb.WriteString("[RowLog Contents 3],[RowLog Contents 4],[Log Record],")
	sb.WriteString("[Offset in Row],[Modify Size]")
	sb.WriteString(" FROM fn_dblog(NULL,NULL)")
	sb.WriteString(" WHERE (")
	sb.WriteString("[Operation] IN (N'LOP_BEGIN_XACT',N'LOP_COMMIT_XACT',N'LOP_ABORT_XACT')")
	sb.WriteString(" OR (")
	sb.WriteString("[Operation] IN (N'LOP_INSERT_ROWS',N'LOP_DELETE_ROWS',N'LOP_MODIFY_ROW',N'LOP_MODIFY_COLUMNS')")
	sb.WriteString(" AND [Context] IN (N'LCX_HEAP',N'LCX_CLUSTERED',N'LCX_MARK_AS_GHOST')")
	sb.WriteString(" AND ISNULL([AllocUnitName],N'') NOT LIKE N'sys.%'")
	sb.WriteString(" AND ISNULL([AllocUnitName],N'') NOT LIKE N'Unknown Alloc Unit%'")
	sb.WriteString("))")
	if afterLSN != "" {
		fmt.Fprintf(&sb, " AND [Current LSN]>N'%s'", escapeSQ(afterLSN))
	}
	return sb.String()
}
