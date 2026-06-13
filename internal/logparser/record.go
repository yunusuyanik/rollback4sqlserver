package logparser

// Operation constants matching SQL Server log operation names.
const (
	OpInsertRows    = "LOP_INSERT_ROWS"
	OpDeleteRows    = "LOP_DELETE_ROWS"
	OpModifyRow     = "LOP_MODIFY_ROW"
	OpModifyColumns = "LOP_MODIFY_COLUMNS"
	OpBeginXact     = "LOP_BEGIN_XACT"
	OpCommitXact    = "LOP_COMMIT_XACT"
	OpAbortXact     = "LOP_ABORT_XACT"
	OpFormatPage    = "LOP_FORMAT_PAGE"
	OpSetBits       = "LOP_SET_BITS"
)

// LogRecord represents one parsed log entry from fn_dblog / fn_dump_dblog.
type LogRecord struct {
	LSN             string // "file:block:slot" hex form, sortable as string
	Operation       string
	Context         string
	TransactionID   string
	TransactionName string
	AllocUnitName   string // e.g. "dbo.Orders" or "dbo.Orders.PK_Orders"
	BeginTime       string // filled for LOP_BEGIN_XACT
	EndTime         string // filled for LOP_COMMIT_XACT / LOP_ABORT_XACT

	// Page/slot addressing — populated from fn_dblog [Page ID], [Slot ID],
	// [AllocUnitId], [PartitionId] columns. Used by the UPDATE MR1 cache.
	PageID      string // e.g. "1:174" (file:page)
	SlotID      *int   // nil when the column is absent from the result set
	AllocUnitID int64
	PartitionID int64

	// Raw row images from fn_dump_dblog / fn_dblog output columns.
	// For INSERT / DELETE: Contents0 = full row image (after for INSERT, before for DELETE).
	// For LOP_MODIFY_ROW full-row update: Contents0 = full after-image, Contents1 = XOR delta.
	// For LOP_MODIFY_ROW in-place delta: Contents0 = new fragment, Contents1 = old fragment.
	Contents0 []byte
	Contents1 []byte
	Contents2 []byte
	Contents3 []byte
	Contents4 []byte

	// RawLogRecord is the complete log record binary from fn_dblog/fn_dump_dblog's
	// [Log Record] column. Used for LOP_MODIFY_ROW offset parsing (SQL Server 2016+).
	RawLogRecord []byte

	// OffsetInRow is the byte offset within the row where a LOP_MODIFY_ROW
	// modification starts. -1 if not provided by fn_dblog (older SQL Server or TRN).
	OffsetInRow int
	// ModifySize is the total number of bytes modified by a LOP_MODIFY_ROW operation.
	// -1 if not provided.
	ModifySize int
}

// IsDataOp returns true for INSERT / DELETE / UPDATE operations.
func (r *LogRecord) IsDataOp() bool {
	switch r.Operation {
	case OpInsertRows, OpDeleteRows, OpModifyRow, OpModifyColumns:
		return true
	}
	return false
}

// IsCommitted is not determined at the record level — callers must correlate
// with LOP_COMMIT_XACT / LOP_ABORT_XACT by TransactionID.
