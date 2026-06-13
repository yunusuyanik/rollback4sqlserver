// Package store — DuckDB backend for log event persistence.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb"

	"github.com/uns/mssqllogrecovery/internal/dml"
)

const (
	duckBatchSize = 500
	ttlDays       = 30
)

// calcDuckDBMaxMemory computes the DuckDB memory limit:
//   - Total system RAM / 20  (5%)
//   - Hard cap at 2 GB
//   - Minimum 256 MB (DuckDB needs headroom for WAL + sort buffers)
func calcDuckDBMaxMemory() string {
	total := totalSystemMemoryBytes()
	if total == 0 {
		return "256MB"
	}
	limit := total / 20
	const (
		maxCap = uint64(2 * 1024 * 1024 * 1024) // 2 GB
		minMB  = uint64(256)
	)
	if limit > maxCap {
		limit = maxCap
	}
	mb := limit / (1024 * 1024)
	if mb < minMB {
		mb = minMB
	}
	return fmt.Sprintf("%dMB", mb)
}

// ResolveDBPath returns the DuckDB file path to use.
// Priority: dataDir flag → ./data/ next to executable.
func ResolveDBPath(dataDir string) string {
	if dataDir != "" {
		return filepath.Join(dataDir, "events.duckdb")
	}
	exePath, err := os.Executable()
	if err != nil {
		return filepath.Join("data", "events.duckdb")
	}
	return filepath.Join(filepath.Dir(exePath), "data", "events.duckdb")
}

// DuckDBStore persists DML statements to an embedded DuckDB database.
type DuckDBStore struct {
	db     *sql.DB
	mu     sync.Mutex
	tx     *sql.Tx
	stmt   *sql.Stmt
	del    *sql.Stmt
	delTxn *sql.Stmt
	count  int

	// pendingCheckpoints holds db_name→lsn pairs to be persisted on the next
	// commitBatch call. Written by SaveCheckpoint, flushed by flushCheckpoints.
	pendingCheckpoints map[string]string
}

// OpenDuckDB opens an embedded DuckDB database.
// Pass "" or ":memory:" for an in-memory database — a shared Connector is used so
// all pool connections see the same data (MVCC: readers never block on the write batch).
// Pass a file path for a persistent database (auto-creates parent directories).
// Memory limit is set dynamically via calcDuckDBMaxMemory().
func OpenDuckDB(path string) (*DuckDBStore, error) {
	var db *sql.DB

	if path == "" || path == ":memory:" {
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			return nil, fmt.Errorf("duckdb connector: %w", err)
		}
		db = sql.OpenDB(connector)
	} else {
		// Auto-create parent directory.
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("duckdb mkdirall: %w", err)
		}
		var err error
		db, err = sql.Open("duckdb", path)
		if err != nil {
			return nil, fmt.Errorf("duckdb open: %w", err)
		}
	}

	// DuckDB MVCC lets readers run concurrently with the open write batch transaction.
	db.SetMaxOpenConns(4)

	// Apply dynamic memory limit: Total RAM / 100, capped at 2 GB.
	maxMem := calcDuckDBMaxMemory()
	if _, err := db.Exec(fmt.Sprintf("PRAGMA max_memory='%s'", maxMem)); err != nil {
		// Non-fatal: log and continue.
		fmt.Fprintf(os.Stderr, "warn: duckdb set max_memory=%s: %v\n", maxMem, err)
	}
	// Use a single thread to reduce per-row contention with the batch writer.
	db.Exec("PRAGMA threads=2")

	s := &DuckDBStore{
		db:                 db,
		pendingCheckpoints: make(map[string]string),
	}
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

// StartTTLWorker starts a background goroutine that deletes log_events older than
// 30 days every hour and checkpoints the WAL to reclaim disk space.
// Call this only for file-based (persistent) DuckDB instances.
func (s *DuckDBStore) StartTTLWorker(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runTTLCleanup()
			}
		}
	}()
}

func (s *DuckDBStore) runTTLCleanup() {
	// Flush pending writes before cleanup so we delete from a committed state.
	if err := s.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "warn: ttl flush: %v\n", err)
		return
	}
	cutoff := fmt.Sprintf("NOW()::TIMESTAMP - INTERVAL '%d DAYS'", ttlDays)
	res, err := s.db.Exec(fmt.Sprintf(
		"DELETE FROM log_events WHERE event_time IS NOT NULL AND event_time < %s", cutoff,
	))
	if err != nil {
		fmt.Fprintf(os.Stderr, "warn: ttl delete: %v\n", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		fmt.Fprintf(os.Stderr, "info: ttl removed %d events older than %d days\n", n, ttlDays)
		s.db.Exec("PRAGMA checkpoint")
	}
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
			event_time   TIMESTAMP,
			commit_time  TIMESTAMP
		)`,
		// checkpoints persists the last-seen LSN per database for restart recovery.
		`CREATE TABLE IF NOT EXISTS checkpoints (
			db_name    VARCHAR NOT NULL PRIMARY KEY,
			lsn        VARCHAR NOT NULL,
			updated_at TIMESTAMP DEFAULT now()
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

	// Migration: add commit_time column if missing (existing databases).
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS commit_time TIMESTAMP`)
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS status VARCHAR`)
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS confidence VARCHAR`)
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS compression_type VARCHAR`)
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS compressed_row_hex VARCHAR`)
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS decompressed_debug_json JSON`)
	s.db.Exec(`ALTER TABLE log_events ADD COLUMN IF NOT EXISTS compression_decode_status VARCHAR`)

	// Remove physical tombstone records written by older versions. These compact
	// 15-byte DELETE payloads are SQL Server cleanup metadata, not row images.
	if _, err := s.db.Exec(`
		DELETE FROM log_events
		WHERE operation = 'DELETE'
		  AND compression_decode_status = 'compressed_row_parse_failed'
		  AND length(compressed_row_hex) = 30
		  AND lower(substr(compressed_row_hex, 1, 18)) IN (
		      '070000000000000000',
		      '4e0000000000000000'
		  )
	`); err != nil {
		return fmt.Errorf("cleanup tombstone migration: %w", err)
	}

	// Migration: ensure UNIQUE(db_name, lsn) to prevent duplicates on restart.
	// Step 1: remove any existing duplicates (idempotent — no-op when table is clean).
	if _, err := s.db.Exec(`
		DELETE FROM log_events
		WHERE id IN (
			SELECT id FROM (
				SELECT id,
				       ROW_NUMBER() OVER (PARTITION BY db_name, lsn ORDER BY id ASC) AS rn
				FROM log_events
			) sub
			WHERE rn > 1
		)
	`); err != nil {
		return fmt.Errorf("dedup migration: %w", err)
	}
	// Step 2: create unique index (no-op if already exists).
	if _, err := s.db.Exec(
		`CREATE UNIQUE INDEX IF NOT EXISTS uq_db_lsn ON log_events(db_name, lsn)`,
	); err != nil {
		return fmt.Errorf("unique index uq_db_lsn: %w", err)
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
	// Re-scans must refresh previously decoded rows. Decoder fixes and refreshed
	// schema metadata can change SQL for the same immutable source LSN.
	del, err := tx.Prepare(`DELETE FROM log_events WHERE db_name = ? AND lsn = ?`)
	if err != nil {
		tx.Rollback()
		return err
	}
	delTxn, err := tx.Prepare(`DELETE FROM log_events WHERE db_name = ? AND txn_id = ?`)
	if err != nil {
		del.Close()
		tx.Rollback()
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO log_events
		(lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time, commit_time,
		 status, confidence, compression_type, compressed_row_hex, decompressed_debug_json, compression_decode_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		delTxn.Close()
		del.Close()
		tx.Rollback()
		return err
	}
	s.tx = tx
	s.stmt = stmt
	s.del = del
	s.delTxn = delTxn
	return nil
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
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
	if isPhysicalCleanupStatement(st) {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	schName, tblName := splitSchemaTable(st.Table)
	var eventTime, commitTime interface{}
	if !st.Timestamp.IsZero() {
		eventTime = st.Timestamp.UTC().Format(time.RFC3339Nano)
	}
	if !st.CommitTime.IsZero() {
		commitTime = st.CommitTime.UTC().Format(time.RFC3339Nano)
	}
	var debugJSON interface{}
	if st.DecompressedDebugJSON != "" {
		debugJSON = st.DecompressedDebugJSON
	}
	if _, err := s.del.Exec(st.Database, st.LSN); err != nil {
		s.restartBatch()
		return err
	}
	if _, err := s.stmt.Exec(
		st.LSN, st.TransactionID, st.Operation,
		st.Database, schName, tblName,
		st.SQL, st.RollbackSQL, eventTime, commitTime,
		nullIfEmpty(st.Status), nullIfEmpty(st.Confidence), nullIfEmpty(st.CompressionType),
		nullIfEmpty(st.CompressedRowHex), debugJSON, nullIfEmpty(st.CompressionDecodeStatus),
	); err != nil {
		s.restartBatch()
		return err
	}
	s.count++
	if s.count >= duckBatchSize {
		return s.commitBatch()
	}
	return nil
}

// DeleteLSN removes a previously persisted event when a newer decoder version
// determines that the source record is not a base-table DML event.
func (s *DuckDBStore) DeleteLSN(dbName, lsn string) error {
	if dbName == "" || lsn == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.del.Exec(dbName, lsn); err != nil {
		s.restartBatch()
		return err
	}
	s.count++
	if s.count >= duckBatchSize {
		return s.commitBatch()
	}
	return nil
}

func (s *DuckDBStore) restartBatch() {
	if s.stmt != nil {
		s.stmt.Close()
	}
	if s.del != nil {
		s.del.Close()
	}
	if s.delTxn != nil {
		s.delTxn.Close()
	}
	if s.tx != nil {
		s.tx.Rollback()
	}
	s.count = 0
	if err := s.beginBatch(); err != nil {
		fmt.Fprintf(os.Stderr, "error: restart duckdb batch: %v\n", err)
	}
}

func isPhysicalCleanupStatement(st *dml.Statement) bool {
	if st == nil ||
		st.Operation != "DELETE" ||
		st.CompressionDecodeStatus != "compressed_row_parse_failed" {
		return false
	}
	rowHex := strings.ToLower(st.CompressedRowHex)
	if len(rowHex) != 30 {
		return false
	}
	return strings.HasPrefix(rowHex, "070000000000000000") ||
		strings.HasPrefix(rowHex, "4e0000000000000000")
}

// DeleteTransaction removes all persisted events for one source transaction.
// Historical rescans use this to clean up rows produced by older decoder
// versions before an internal SQL Server maintenance transaction was excluded.
func (s *DuckDBStore) DeleteTransaction(dbName, txnID string) error {
	if dbName == "" || txnID == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.delTxn.Exec(dbName, txnID); err != nil {
		return err
	}
	s.count++
	if s.count >= duckBatchSize {
		return s.commitBatch()
	}
	return nil
}

// SaveCheckpoint records dbName's last-seen LSN for persistence.
// The value is written to the checkpoints table on the next commitBatch / Flush.
func (s *DuckDBStore) SaveCheckpoint(dbName, lsn string) {
	s.mu.Lock()
	s.pendingCheckpoints[dbName] = lsn
	s.mu.Unlock()
}

// LoadAllCheckpoints returns all persisted db_name → lsn pairs.
// Called once on startup to restore the in-memory checkpoint state.
func (s *DuckDBStore) LoadAllCheckpoints() (map[string]string, error) {
	rows, err := s.db.Query(`SELECT db_name, lsn FROM checkpoints`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		var dbName, lsn string
		if err := rows.Scan(&dbName, &lsn); err != nil {
			return nil, err
		}
		result[dbName] = lsn
	}
	return result, rows.Err()
}

func (s *DuckDBStore) commitBatch() error {
	s.stmt.Close()
	s.del.Close()
	s.delTxn.Close()
	if err := s.tx.Commit(); err != nil {
		return err
	}
	s.count = 0
	// Persist pending checkpoints after the log_events transaction commits.
	// Running after commit avoids concurrent write conflicts with the batch tx.
	if len(s.pendingCheckpoints) > 0 {
		if err := s.flushCheckpoints(); err != nil {
			// Non-fatal: checkpoint will be retried on next commitBatch.
			fmt.Fprintf(os.Stderr, "warn: persist checkpoints: %v\n", err)
		}
	}
	return s.beginBatch()
}

// flushCheckpoints upserts all pending db→lsn pairs into the checkpoints table.
// Must be called after the log_events batch transaction is committed.
// Caller must hold s.mu.
func (s *DuckDBStore) flushCheckpoints() error {
	for dbName, lsn := range s.pendingCheckpoints {
		_, err := s.db.Exec(`
			INSERT INTO checkpoints (db_name, lsn, updated_at) VALUES (?, ?, now())
			ON CONFLICT (db_name) DO UPDATE SET lsn = excluded.lsn, updated_at = now()
		`, dbName, lsn)
		if err != nil {
			return err
		}
	}
	s.pendingCheckpoints = make(map[string]string)
	return nil
}

// Flush commits any buffered writes.
func (s *DuckDBStore) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count > 0 {
		return s.commitBatch()
	}
	// Still flush pending checkpoints even when there are no buffered events.
	if len(s.pendingCheckpoints) > 0 {
		if err := s.flushCheckpoints(); err != nil {
			fmt.Fprintf(os.Stderr, "warn: persist checkpoints (flush): %v\n", err)
		}
	}
	return nil
}

// Reset discards buffered writes, deletes all rows, and starts a fresh batch.
// Used by interactive scans (/api/start) to clear results before a new scan.
// Does NOT clear the checkpoints table — auto-scan checkpoints survive a reset.
func (s *DuckDBStore) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stmt != nil {
		s.stmt.Close()
		s.stmt = nil
	}
	if s.del != nil {
		s.del.Close()
		s.del = nil
	}
	if s.delTxn != nil {
		s.delTxn.Close()
		s.delTxn = nil
	}
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
	s.count = 0
	s.pendingCheckpoints = make(map[string]string)
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
	if s.del != nil {
		s.del.Close()
	}
	if s.delTxn != nil {
		s.delTxn.Close()
	}
	return s.db.Close()
}
