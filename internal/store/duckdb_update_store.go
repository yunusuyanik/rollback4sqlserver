package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/uns/mssqllogrecovery/internal/mssql"
)

// DuckDBUpdateStore persists UPDATE events to the mssql_log_updates table.
//
// It uses auto-commit inserts on the provided *sql.DB. When the same DB is
// also used by DuckDBStore (which keeps an open write transaction), callers
// must call DuckDBStore.Flush() before writing update events to avoid
// write-write conflicts (DuckDB allows only one concurrent writer).
type DuckDBUpdateStore struct {
	db *sql.DB
	mu sync.Mutex
}

// NewDuckDBUpdateStore creates the store and ensures mssql_log_updates exists.
func NewDuckDBUpdateStore(db *sql.DB) (*DuckDBUpdateStore, error) {
	s := &DuckDBUpdateStore{db: db}
	if err := s.setupSchema(); err != nil {
		return nil, fmt.Errorf("update store schema: %w", err)
	}
	return s, nil
}

func (s *DuckDBUpdateStore) setupSchema() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS mssql_log_updates (
		id                   VARCHAR PRIMARY KEY,
		source_database      VARCHAR,
		schema_name          VARCHAR,
		table_name           VARCHAR,
		current_lsn          VARCHAR,
		transaction_id       VARCHAR,
		operation            VARCHAR,
		context              VARCHAR,
		page_id              VARCHAR,
		slot_id              INTEGER,
		alloc_unit_id        BIGINT,
		partition_id         BIGINT,

		rowlog0_hex          VARCHAR,
		rowlog1_hex          VARCHAR,
		rowlog2_hex          VARCHAR,
		rowlog3_hex          VARCHAR,

		mr1_hex              VARCHAR,
		mr0_hex              VARCHAR,

		before_json          VARCHAR,
		after_json           VARCHAR,
		changed_columns_json VARCHAR,

		equivalent_redo_sql  VARCHAR,
		equivalent_undo_sql  VARCHAR,

		status               VARCHAR,
		confidence           VARCHAR,
		error_message        VARCHAR,
		debug_json           VARCHAR,

		created_at           TIMESTAMP
	)`)
	return err
}

// WriteUpdateEvent inserts one UPDATE event. Generates a UUID for the id column.
func (s *DuckDBUpdateStore) WriteUpdateEvent(ctx context.Context, evt mssql.UpdateEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := uuid.New().String()

	var slotID interface{}
	if evt.SlotID != nil {
		slotID = *evt.SlotID
	}
	var createdAt interface{}
	if !evt.CreatedAt.IsZero() {
		createdAt = evt.CreatedAt.UTC().Format(time.RFC3339Nano)
	}

	_, err := s.db.ExecContext(ctx, `INSERT INTO mssql_log_updates (
		id, source_database, schema_name, table_name,
		current_lsn, transaction_id, operation, context,
		page_id, slot_id, alloc_unit_id, partition_id,
		rowlog0_hex, rowlog1_hex, rowlog2_hex, rowlog3_hex,
		mr1_hex, mr0_hex,
		before_json, after_json, changed_columns_json,
		equivalent_redo_sql, equivalent_undo_sql,
		status, confidence, error_message, debug_json,
		created_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT (id) DO NOTHING`,
		id, evt.SourceDB, evt.SchemaName, evt.TableName,
		evt.CurrentLSN, evt.TransactionID, evt.Operation, evt.Context,
		evt.PageID, slotID, evt.AllocUnitID, evt.PartitionID,
		evt.RowLog0Hex, evt.RowLog1Hex, evt.RowLog2Hex, evt.RowLog3Hex,
		evt.MR1Hex, evt.MR0Hex,
		evt.BeforeJSON, evt.AfterJSON, evt.ChangedColumnsJSON,
		evt.EquivalentRedoSQL, evt.EquivalentUndoSQL,
		evt.Status, evt.Confidence, evt.ErrorMessage, evt.DebugJSON,
		createdAt,
	)
	return err
}
