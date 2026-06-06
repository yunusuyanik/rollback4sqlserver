// Package store persists log events in an embedded SQLite database.
// Uses modernc.org/sqlite — pure Go, no C compiler required.
package store

import (
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"

	"github.com/uns/mssqllogrecovery/internal/dml"
)

const ddl = `
CREATE TABLE IF NOT EXISTS log_events (
	id         INTEGER PRIMARY KEY AUTOINCREMENT,
	lsn        TEXT NOT NULL,
	txn_id     TEXT,
	operation  TEXT,
	table_name TEXT,
	sql_stmt   TEXT,
	created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_table ON log_events(table_name);
CREATE INDEX IF NOT EXISTS idx_op    ON log_events(operation);
CREATE INDEX IF NOT EXISTS idx_lsn   ON log_events(lsn);
`

const batchSize = 500

// SQLiteStore persists DML statements to a local SQLite file.
type SQLiteStore struct {
	db    *sql.DB
	mu    sync.Mutex
	tx    *sql.Tx
	stmt  *sql.Stmt
	count int
}

// Open opens (or creates) a SQLite database at path.
// Pass ":memory:" for an in-memory database; it will be opened as a named
// shared-cache URI so multiple connections (reads + write batch) can coexist.
func Open(path string) (*SQLiteStore, error) {
	dsn := path
	if path == ":memory:" {
		// Named shared-cache in-memory DB: multiple connections see the same data.
		dsn = "file::memory:?cache=shared"
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// Allow concurrent readers alongside the batch write transaction.
	db.SetMaxOpenConns(10)
	if _, err := db.Exec(ddl); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite init: %w", err)
	}
	s := &SQLiteStore{db: db}
	if err := s.beginBatch(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// DB returns the underlying *sql.DB for ad-hoc queries (e.g. from the web server).
func (s *SQLiteStore) DB() *sql.DB { return s.db }

func (s *SQLiteStore) beginBatch() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO log_events (lsn, txn_id, operation, table_name, sql_stmt)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	s.tx = tx
	s.stmt = stmt
	return nil
}

// Write buffers a statement; commits automatically every batchSize records.
func (s *SQLiteStore) Write(st *dml.Statement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.stmt.Exec(st.LSN, st.TransactionID, st.Operation, st.Table, st.SQL); err != nil {
		return err
	}
	s.count++
	if s.count >= batchSize {
		return s.commitBatch()
	}
	return nil
}

func (s *SQLiteStore) commitBatch() error {
	s.stmt.Close()
	if err := s.tx.Commit(); err != nil {
		return err
	}
	s.count = 0
	return s.beginBatch()
}

// Flush commits any buffered writes.
func (s *SQLiteStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		return s.commitBatch()
	}
	return nil
}

// Reset discards buffered writes, deletes all rows, and starts a fresh batch.
// Use this to clear results before a new scan without closing the store.
func (s *SQLiteStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Release the open batch transaction so the connection returns to the pool.
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
func (s *SQLiteStore) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if s.stmt != nil {
		s.stmt.Close()
	}
	return s.db.Close()
}
