// Package store — DuckDB backend for log event persistence.
package store

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb"

	"github.com/uns/mssqllogrecovery/internal/dml"
)

const duckBatchSize = 500

// DuckDBStore persists DML statements to an embedded DuckDB database.
type DuckDBStore struct {
	db    *sql.DB
	mu    sync.Mutex
	tx    *sql.Tx
	stmt  *sql.Stmt
	count int
}

// OpenDuckDB opens an embedded DuckDB database.
// Pass "" or ":memory:" for an in-memory database — a shared Connector is used so
// all pool connections see the same data (MVCC: readers never block on the write batch).
// Pass a file path for a persistent database.
func OpenDuckDB(path string) (*DuckDBStore, error) {
	var db *sql.DB

	if path == "" || path == ":memory:" {
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			return nil, fmt.Errorf("duckdb connector: %w", err)
		}
		db = sql.OpenDB(connector)
	} else {
		var err error
		db, err = sql.Open("duckdb", path)
		if err != nil {
			return nil, fmt.Errorf("duckdb open: %w", err)
		}
	}

	// DuckDB MVCC lets readers run concurrently with the open write batch transaction.
	db.SetMaxOpenConns(4)

	s := &DuckDBStore{db: db}
	if err := s.setupSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("duckdb schema: %w", err)
	}
	if err := s.beginBatch(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *DuckDBStore) setupSchema() error {
	stmts := []string{
		`CREATE SEQUENCE IF NOT EXISTS log_events_seq START 1`,
		`CREATE TABLE IF NOT EXISTS log_events (
			id           BIGINT DEFAULT nextval('log_events_seq') PRIMARY KEY,
			lsn          VARCHAR NOT NULL,
			txn_id       VARCHAR,
			operation    VARCHAR,
			db_name      VARCHAR,
			schema_name  VARCHAR,
			table_name   VARCHAR,
			primary_key  VARCHAR,
			sql_stmt     VARCHAR,
			rollback_sql VARCHAR,
			event_time   TIMESTAMP
		)`,
		`CREATE INDEX IF NOT EXISTS idx_table  ON log_events(table_name)`,
		`CREATE INDEX IF NOT EXISTS idx_schema ON log_events(schema_name)`,
		`CREATE INDEX IF NOT EXISTS idx_db     ON log_events(db_name)`,
		`CREATE INDEX IF NOT EXISTS idx_op     ON log_events(operation)`,
		`CREATE INDEX IF NOT EXISTS idx_lsn    ON log_events(lsn)`,
		`CREATE INDEX IF NOT EXISTS idx_time   ON log_events(event_time)`,
	}
	for _, ddl := range stmts {
		if _, err := s.db.Exec(ddl); err != nil {
			return err
		}
	}
	return nil
}

// DB returns the underlying *sql.DB for ad-hoc read queries (e.g. from the HTTP server).
func (s *DuckDBStore) DB() *sql.DB { return s.db }

func (s *DuckDBStore) beginBatch() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO log_events
		(lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	s.tx = tx
	s.stmt = stmt
	return nil
}

// splitSchemaTable splits "[dbo].[Orders]" or "dbo.Orders" → ("dbo", "Orders").
// Strips SQL brackets before splitting. Defaults to schema "dbo" when no dot is present.
func splitSchemaTable(full string) (schemaName, tableName string) {
	full = strings.NewReplacer("[", "", "]", "").Replace(full)
	if idx := strings.IndexByte(full, '.'); idx >= 0 {
		return full[:idx], full[idx+1:]
	}
	return "dbo", full
}

// Write buffers a statement; commits automatically every batchSize records.
func (s *DuckDBStore) Write(st *dml.Statement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	schName, tblName := splitSchemaTable(st.Table)
	var eventTime interface{}
	if !st.Timestamp.IsZero() {
		eventTime = st.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if _, err := s.stmt.Exec(
		st.LSN, st.TransactionID, st.Operation,
		st.Database, schName, tblName,
		st.SQL, st.RollbackSQL, eventTime,
	); err != nil {
		return err
	}
	s.count++
	if s.count >= duckBatchSize {
		return s.commitBatch()
	}
	return nil
}

func (s *DuckDBStore) commitBatch() error {
	s.stmt.Close()
	if err := s.tx.Commit(); err != nil {
		return err
	}
	s.count = 0
	return s.beginBatch()
}

// Flush commits any buffered writes.
func (s *DuckDBStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		return s.commitBatch()
	}
	return nil
}

// Reset discards buffered writes, deletes all rows, and starts a fresh batch.
func (s *DuckDBStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt != nil {
		s.stmt.Close()
		s.stmt = nil
	}
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
	s.count = 0
	if _, err := s.db.Exec(`DELETE FROM log_events`); err != nil {
		return err
	}
	return s.beginBatch()
}

// Close flushes and closes the database.
func (s *DuckDBStore) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if s.stmt != nil {
		s.stmt.Close()
	}
	return s.db.Close()
}
