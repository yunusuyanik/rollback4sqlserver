package logparser

// Operation constants matching SQL Server log operation names.
const (
	OpInsertRows     = "LOP_INSERT_ROWS"
	OpDeleteRows     = "LOP_DELETE_ROWS"
	OpModifyRow      = "LOP_MODIFY_ROW"
	OpModifyColumns  = "LOP_MODIFY_COLUMNS"
	OpBeginXact      = "LOP_BEGIN_XACT"
	OpCommitXact     = "LOP_COMMIT_XACT"
	OpAbortXact      = "LOP_ABORT_XACT"
	OpFormatPage     = "LOP_FORMAT_PAGE"
	OpSetBits        = "LOP_SET_BITS"
)

// LogRecord represents one parsed log entry from fn_dblog / fn_dump_dblog.
type LogRecord struct {
	LSN           string // "file:block:slot" hex form, sortable as string
	Operation     string
	Context       string
	TransactionID string
	AllocUnitName string // e.g. "dbo.Orders" or "dbo.Orders.PK_Orders"
	BeginTime     string // filled for LOP_BEGIN_XACT
	EndTime       string // filled for LOP_COMMIT_XACT / LOP_ABORT_XACT

	// Raw row images from fn_dump_dblog / fn_dblog output columns.
	// Contents0: after-image for INSERT/UPDATE; before-image for DELETE.
	// Contents1: for LOP_MODIFY_ROW — XOR delta (before XOR after) for changed bytes.
	Contents0 []byte
	Contents1 []byte
	Contents2 []byte
	Contents3 []byte
	Contents4 []byte
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
