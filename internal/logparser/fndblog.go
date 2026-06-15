package logparser

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/microsoft/go-mssqldb"
)

// LDFReader reads live transaction log records from fn_dblog on a running
// SQL Server instance. It always calls fn_dblog(NULL,NULL) and applies the LSN
// cursor in the outer WHERE clause. Some SQL Server builds reject otherwise
// valid record LSNs as fn_dblog's OpenRowset start parameter.
type LDFReader struct {
	db        *sql.DB
	startLSN  string // exclusive lower bound ("" = from active-log head)
	chunkSize int
	onChunk   func(afterLSN, lastLSN string, count int)
}

const defaultLogChunkSize = 5000

// NewLDFReader constructs a reader against the given connection.
func NewLDFReader(db *sql.DB) *LDFReader {
	return &LDFReader{db: db, chunkSize: configuredLogChunkSize()}
}

// WithStartLSN sets an exclusive lower bound. Records with [Current LSN] <= lsn
// are not returned. Use "" to read from the beginning of the active log.
func (r *LDFReader) WithStartLSN(lsn string) *LDFReader {
	r.startLSN = lsn
	return r
}

// WithChunkSize sets the maximum number of log records returned by one SQL
// query. Values <= 0 disable pagination.
func (r *LDFReader) WithChunkSize(size int) *LDFReader {
	r.chunkSize = size
	return r
}

// WithChunkObserver registers a callback after each SQL page is consumed.
func (r *LDFReader) WithChunkObserver(fn func(afterLSN, lastLSN string, count int)) *LDFReader {
	r.onChunk = fn
	return r
}

// Read streams relevant log records in bounded LSN pages. The same handler is
// used across pages, so transaction state can span chunk boundaries.
func (r *LDFReader) Read(fn func(*LogRecord) error) error {
	cursor := r.startLSN
	for {
		count, lastLSN, err := r.readChunk(cursor, fn)
		if err != nil {
			return err
		}
		if count > 0 && r.onChunk != nil {
			r.onChunk(cursor, lastLSN, count)
		}
		if count == 0 || r.chunkSize <= 0 || count < r.chunkSize {
			return nil
		}
		if lastLSN == "" || lastLSN == cursor {
			return fmt.Errorf("fn_dblog pagination did not advance past %q", cursor)
		}
		cursor = lastLSN
	}
}

func (r *LDFReader) readChunk(afterLSN string, fn func(*LogRecord) error) (int, string, error) {
	rows, err := r.db.Query(buildLiveChunkQuery(afterLSN, r.chunkSize))
	if err != nil {
		return 0, "", fmt.Errorf("fn_dblog: %w", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return 0, "", err
	}
	idx := columnIndex(colNames)

	count := 0
	var lastLSN string
	for rows.Next() {
		rec, err := scanRecord(rows, colNames, idx)
		if err != nil {
			return count, lastLSN, err
		}
		if err := fn(rec); err != nil {
			return count, lastLSN, err
		}
		count++
		lastLSN = rec.LSN
	}
	return count, lastLSN, rows.Err()
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
	return buildLiveChunkQuery(afterLSN, 0)
}

func buildLiveChunkQuery(afterLSN string, chunkSize int) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if chunkSize > 0 {
		fmt.Fprintf(&sb, "TOP (%d) ", chunkSize)
	}
	sb.WriteString("[Current LSN],[Operation],[Context],[Transaction ID],[Transaction Name],")
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
	sb.WriteString(" ORDER BY [Current LSN]")
	return sb.String()
}

func configuredLogChunkSize() int {
	size, err := strconv.Atoi(strings.TrimSpace(os.Getenv("LOGRECOVERY_LOG_CHUNK_SIZE")))
	if err == nil && size > 0 {
		return size
	}
	return defaultLogChunkSize
}
