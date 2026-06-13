package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/uns/mssqllogrecovery/internal/dml"
	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/mssql"
	"github.com/uns/mssqllogrecovery/internal/schema"
	"github.com/uns/mssqllogrecovery/internal/store"
)

// ── Types ─────────────────────────────────────────────────────────────────────

type ScanProgress struct {
	Running bool   `json:"running"`
	Done    bool   `json:"done"`
	Inserts int64  `json:"inserts"`
	Updates int64  `json:"updates"`
	Deletes int64  `json:"deletes"`
	Other   int64  `json:"other"`
	Errors  int64  `json:"errors"`
	Total   int64  `json:"total"`
	Message string `json:"message"`
	ErrMsg  string `json:"error,omitempty"`
}

// ── App state ─────────────────────────────────────────────────────────────────

var (
	appMu      sync.RWMutex
	appStore   *store.DuckDBStore
	appSchemas = map[string]*schema.Schema{} // keyed by database name

	scanMu   sync.Mutex
	scanning atomic.Bool
	progress ScanProgress
	progMu   sync.RWMutex

	sessMu                             sync.RWMutex
	sessServer, sessUser, sessPassword string

	// LSN checkpoint per database for incremental LDF polling.
	checkpointMu  sync.RWMutex
	ldfCheckpoint = map[string]string{}

	// pollState persists pending transaction state across poll cycles so that
	// transactions whose INSERT/UPDATE/DELETE records arrive in one cycle but
	// whose COMMIT arrives in the next cycle are not silently dropped.
	pollStateMu   sync.Mutex
	pollStateByDB = map[string]*dbPollState{}

	// Context controlling all auto-scan / poller goroutines.
	bgCtx    context.Context
	bgCancel context.CancelFunc
)

// ── Console logger ────────────────────────────────────────────────────────────

// logf prints a timestamped line to stdout: [15:04:05] message
func logf(format string, args ...interface{}) {
	ts := time.Now().Format("15:04:05")
	fmt.Printf("[%s] %s\n", ts, fmt.Sprintf(format, args...))
}

// fmtN formats an integer with comma separators (1247 → "1,247").
func fmtN(n int64) string {
	s := fmt.Sprintf("%d", n)
	neg := n < 0
	if neg {
		s = s[1:]
	}
	out := make([]byte, 0, len(s)+(len(s)-1)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

// shortLSN trims a full LSN "00000022:000001A1:0001" to "22:1A1" for compact display.
func shortLSN(lsn string) string {
	if lsn == "" {
		return "?"
	}
	parts := strings.SplitN(lsn, ":", 3)
	if len(parts) < 2 {
		return lsn
	}
	a := strings.TrimLeft(parts[0], "0")
	if a == "" {
		a = "0"
	}
	b := strings.TrimLeft(parts[1], "0")
	if b == "" {
		b = "0"
	}
	return a + ":" + b
}

func runServe(httpPort int, host, user, pass string, sqlPort int, dbName string, allDBs bool, sinceTime time.Time, dataDir string) error {
	bgCtx, bgCancel = context.WithCancel(context.Background())

	dbPath := store.ResolveDBPath(dataDir)
	var err error
	appStore, err = store.OpenDuckDB(dbPath)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}
	logf("persistent store: %s", dbPath)
	appStore.StartTTLWorker(bgCtx)
	if cps, err2 := appStore.LoadAllCheckpoints(); err2 == nil {
		checkpointMu.Lock()
		for db, lsn := range cps {
			ldfCheckpoint[db] = lsn
		}
		checkpointMu.Unlock()
		if len(cps) > 0 {
			logf("restored %d checkpoint(s) — incremental scan will resume", len(cps))
		}
	}

	if host != "" {
		server := buildServerAddr(host, sqlPort)
		sessMu.Lock()
		sessServer = server
		sessUser = user
		sessPassword = pass
		sessMu.Unlock()

		if dbName != "" || allDBs {
			go runAutoScan(server, user, pass, dbName, allDBs, sinceTime)
		} else {
			logf("no --db or --all-dbs specified — connect via UI or add flag")
		}
	} else {
		logf("no --host specified — waiting for browser connection")
	}

	addr := fmt.Sprintf(":%d", httpPort)
	logf("API ready → http://localhost%s", addr)
	logf("open https://rollback4sqlserver.dbaops.io and connect to http://localhost%s", addr)
	return http.ListenAndServe(addr, buildMux())
}

// ── Poll state ───────────────────────────────────────────────────────────────

// dbPollState persists transaction-level bookkeeping for one database's LDF
// poller across poll cycles. A transaction whose data records arrive in cycle N
// and whose COMMIT arrives in cycle N+1 is correctly flushed thanks to this state.
type dbPollState struct {
	pending       map[string][]*dml.Statement
	beginTimes    map[string]time.Time
	excluded      map[string]bool
	schemaVersion string
}

func getPollState(dbName string) *dbPollState {
	pollStateMu.Lock()
	defer pollStateMu.Unlock()
	if s, ok := pollStateByDB[dbName]; ok {
		return s
	}
	s := &dbPollState{
		pending:    make(map[string][]*dml.Statement),
		beginTimes: make(map[string]time.Time),
		excluded:   make(map[string]bool),
	}
	pollStateByDB[dbName] = s
	return s
}

// pruneStalePollState discards pending entries for transactions that started
// more than 2 hours ago and never committed (avoids unbounded growth).
func pruneStalePollState(state *dbPollState) {
	cutoff := time.Now().Add(-2 * time.Hour)
	for txnID, bt := range state.beginTimes {
		if !bt.IsZero() && bt.Before(cutoff) {
			delete(state.pending, txnID)
			delete(state.beginTimes, txnID)
			delete(state.excluded, txnID)
		}
	}
}

// ── Auto-scan orchestration ───────────────────────────────────────────────────

func runAutoScan(server, user, pass, dbName string, allDBs bool, sinceTime time.Time) {
	setProgress(func(p *ScanProgress) { p.Running = true; p.Message = "Connecting…" })

	logf("connecting to SQL Server at %s", server)
	dbs, err := resolveTargetDBs(server, user, pass, dbName, allDBs)
	if err != nil {
		logf("ERROR: %v", err)
		setProgress(func(p *ScanProgress) { p.Running = false; p.Done = true; p.ErrMsg = err.Error() })
		return
	}
	logf("connected. target database(s): %s", strings.Join(dbs, ", "))
	if !sinceTime.IsZero() {
		logf("scanning changes since %s", sinceTime.Format("2006-01-02 15:04:05"))
	} else {
		logf("scanning full available log history")
	}

	for _, db := range dbs {
		select {
		case <-bgCtx.Done():
			return
		default:
		}
		setProgress(func(p *ScanProgress) { p.Message = fmt.Sprintf("Initial scan: %s…", db) })
		if err := scanDatabase(bgCtx, server, user, pass, db, sinceTime); err != nil && err != context.Canceled {
			logf("ERROR [%s]: %v", db, err)
			setProgress(func(p *ScanProgress) { p.ErrMsg = fmt.Sprintf("%s: %v", db, err) })
		}
	}

	appMu.RLock()
	st := appStore
	appMu.RUnlock()
	if err := st.Flush(); err != nil {
		logf("ERROR flush: %v", err)
		setProgress(func(p *ScanProgress) { p.ErrMsg = "flush: " + err.Error() })
	}

	progMu.RLock()
	total := progress.Total
	progMu.RUnlock()
	logf("initial scan complete: %s events imported", fmtN(total))
	logf("polling live log every 5s for %d database(s)", len(dbs))

	setProgress(func(p *ScanProgress) {
		p.Message = fmt.Sprintf("Polling live log every 5s — %s events loaded", fmtN(p.Total))
	})

	for _, db := range dbs {
		db := db
		go startLDFPoller(bgCtx, server, user, pass, db)
	}
}

func resolveTargetDBs(server, user, password, dbName string, allDBs bool) ([]string, error) {
	if !allDBs {
		if dbName == "" {
			return nil, fmt.Errorf("specify --db <name> or --all-dbs")
		}
		return []string{dbName}, nil
	}
	db, err := openConnection(server, user, password, "master")
	if err != nil {
		return nil, fmt.Errorf("connect to master: %w", err)
	}
	defer db.Close()
	rows, err := db.Query(
		`SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dbs []string
	for rows.Next() {
		var n string
		rows.Scan(&n)
		dbs = append(dbs, n)
	}
	if len(dbs) == 0 {
		return nil, fmt.Errorf("no online user databases found")
	}
	return dbs, nil
}

// scanDatabase does a one-time committed-only LDF scan bounded by sinceTime.
// On restart it resumes from the persisted LSN checkpoint, skipping events
// already stored in the DB. The UNIQUE(db_name, lsn) constraint provides a
// second line of defence in case of overlap.
func scanDatabase(ctx context.Context, server, user, pass, dbName string, sinceTime time.Time) error {
	srcDB, err := openConnection(server, user, pass, dbName)
	if err != nil {
		return err
	}
	defer srcDB.Close()

	sch, err := schema.Extract(srcDB)
	if err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	logf("[%s] schema: %d tables", dbName, len(sch.Tables))
	appMu.Lock()
	appSchemas[dbName] = sch
	appMu.Unlock()
	if version, versionErr := readSchemaVersion(srcDB); versionErr == nil {
		getPollState(dbName).schemaVersion = version
	}

	appMu.RLock()
	st := appStore
	appMu.RUnlock()

	// A requested history window must always be re-read from the active log.
	// Checkpoints are only an optimization for unbounded incremental scans;
	// using one here would skip transactions that are inside sinceTime but
	// older than the last poller's checkpoint.
	checkpointMu.RLock()
	resumeLSN := ldfCheckpoint[dbName]
	checkpointMu.RUnlock()
	effectiveSince := sinceTime
	if !sinceTime.IsZero() {
		resumeLSN = ""
	}

	if resumeLSN != "" {
		logf("[%s] resuming from LSN %s", dbName, resumeLSN)
	} else {
		logf("[%s] starting initial scan", dbName)
	}

	gen := dml.NewWithPageReader(sch, mssql.NewSQLPageReader(srcDB, dbName))
	pending := make(map[string][]*dml.Statement)
	beginTimes := make(map[string]time.Time)
	excluded := make(map[string]bool)
	seenLSNs := make(map[string]struct{})
	var firstLSN, maxLSN string
	var beginCount, dataCount, commitCount, generatedCount, generateErrorCount int64
	var eligibleContextCount, resolvedTableCount int64

	progMu.RLock()
	snapIns, snapUpd, snapDel, snapTotal := progress.Inserts, progress.Updates, progress.Deletes, progress.Total
	progMu.RUnlock()

	var lastLog time.Time
	var lastLoggedTotal int64

	handler := func(rec *logparser.LogRecord) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		if _, seen := seenLSNs[rec.LSN]; seen {
			return nil
		}
		seenLSNs[rec.LSN] = struct{}{}
		if firstLSN == "" {
			firstLSN = rec.LSN
		}
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		switch rec.Operation {
		case logparser.OpBeginXact:
			beginCount++
		case logparser.OpCommitXact:
			commitCount++
		case logparser.OpInsertRows, logparser.OpDeleteRows, logparser.OpModifyRow, logparser.OpModifyColumns:
			dataCount++
			if rec.Context == "LCX_HEAP" || rec.Context == "LCX_CLUSTERED" || rec.Context == "LCX_MARK_AS_GHOST" {
				eligibleContextCount++
				if sch.LookupStorage(rec.AllocUnitName, rec.AllocUnitID, rec.PartitionID) != nil {
					resolvedTableCount++
				}
			}
		}
		beforePending := pendingStatementCount(pending)
		err := handleScanRecord(rec, gen, dbName, effectiveSince, st, pending, beginTimes, excluded, true, func(string) bool { return true })
		afterPending := pendingStatementCount(pending)
		if afterPending > beforePending {
			generatedCount += int64(afterPending - beforePending)
		}
		if err != nil {
			generateErrorCount++
			if generateErrorCount <= 10 {
				logf("[%s] decode error: %v", dbName, err)
			}
			return nil
		}
		if time.Since(lastLog) >= time.Second {
			progMu.RLock()
			tot := progress.Total
			errs := progress.Errors
			progMu.RUnlock()
			if tot > lastLoggedTotal {
				logf("[%s] imported %s events · %d errors", dbName, fmtN(tot), errs)
				lastLog = time.Now()
				lastLoggedTotal = tot
			}
		}
		return nil
	}

	if !sinceTime.IsZero() {
		files, backupErr := historicalLogBackupFiles(ctx, srcDB, dbName, sinceTime)
		switch {
		case backupErr != nil:
			logf("[%s] historical TRN discovery skipped: %v", dbName, backupErr)
		case len(files) == 0:
			logf("[%s] no accessible TRN backups found since %s; history before active LDF is unavailable",
				dbName, sinceTime.Format("2006-01-02 15:04:05"))
		default:
			logf("[%s] historical scan: reading %d TRN backup file(s)", dbName, len(files))
			if err := logparser.NewTRNReader(srcDB, files).Read(scanOps(), handler); err != nil && err != context.Canceled {
				logf("[%s] historical TRN scan failed: %v", dbName, err)
			}
		}
	}

	r := logparser.NewLDFReader(srcDB)
	if resumeLSN != "" {
		r = r.WithStartLSN(resumeLSN)
	}
	if err := r.Read(handler); err != nil && err != context.Canceled {
		return err
	}
	if maxLSN != "" {
		checkpointMu.Lock()
		ldfCheckpoint[dbName] = maxLSN
		checkpointMu.Unlock()
		if st != nil {
			st.SaveCheckpoint(dbName, maxLSN)
		}
	}

	progMu.RLock()
	deltaTotal := progress.Total - snapTotal
	deltaIns := progress.Inserts - snapIns
	deltaUpd := progress.Updates - snapUpd
	deltaDel := progress.Deletes - snapDel
	deltaErrs := progress.Errors
	progMu.RUnlock()
	logf("[%s] imported %s (ins:%s upd:%s del:%s) · %d errors · LSN %s→%s",
		dbName, fmtN(deltaTotal), fmtN(deltaIns), fmtN(deltaUpd), fmtN(deltaDel),
		deltaErrs, shortLSN(firstLSN), shortLSN(maxLSN))
	logf("[%s] scan detail: begin=%d data=%d eligible_context=%d resolved_table=%d commit=%d generated=%d pending=%d generator_errors=%d",
		dbName, beginCount, dataCount, eligibleContextCount, resolvedTableCount, commitCount, generatedCount,
		pendingStatementCount(pending), generateErrorCount)
	return nil
}

func pendingStatementCount(pending map[string][]*dml.Statement) int {
	total := 0
	for _, statements := range pending {
		total += len(statements)
	}
	return total
}

// historicalLogBackupFiles returns SQL Server-side TRN paths whose backup time
// overlaps the requested history window. One immediately preceding backup is
// included so transactions that began before the boundary but committed inside
// it still have their BEGIN and row records available.
func historicalLogBackupFiles(
	ctx context.Context,
	srcDB *sql.DB,
	dbName string,
	sinceTime time.Time,
) ([]string, error) {
	rows, err := srcDB.QueryContext(ctx, `
		WITH selected_backups AS (
			SELECT TOP (512)
				bs.backup_set_id,
				bs.backup_start_date
			FROM msdb.dbo.backupset bs
			WHERE bs.database_name = @db
			  AND bs.type = 'L'
			  AND bs.backup_finish_date >= @since
			ORDER BY bs.backup_start_date
		),
		previous_backup AS (
			SELECT TOP (1)
				bs.backup_set_id,
				bs.backup_start_date
			FROM msdb.dbo.backupset bs
			WHERE bs.database_name = @db
			  AND bs.type = 'L'
			  AND bs.backup_finish_date < @since
			ORDER BY bs.backup_finish_date DESC
		),
		wanted AS (
			SELECT * FROM previous_backup
			UNION
			SELECT * FROM selected_backups
		)
		SELECT bmf.physical_device_name
		FROM wanted w
		JOIN msdb.dbo.backupset bs
		  ON bs.backup_set_id = w.backup_set_id
		JOIN msdb.dbo.backupmediafamily bmf
		  ON bmf.media_set_id = bs.media_set_id
		ORDER BY w.backup_start_date, bmf.family_sequence_number`,
		sql.Named("db", dbName),
		sql.Named("since", sinceTime),
	)
	if err != nil {
		return nil, fmt.Errorf("msdb log backup query: %w", err)
	}
	defer rows.Close()

	var files []string
	seen := make(map[string]bool)
	for rows.Next() {
		var path sql.NullString
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		if path.Valid && path.String != "" && !seen[path.String] {
			files = append(files, path.String)
			seen[path.String] = true
		}
	}
	return files, rows.Err()
}

// startLDFPoller polls fn_dblog every 5 s, storing committed changes since last LSN.
func startLDFPoller(ctx context.Context, server, user, pass, dbName string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkpointMu.RLock()
			lsnBefore := ldfCheckpoint[dbName]
			checkpointMu.RUnlock()

			progMu.RLock()
			snapTotal := progress.Total
			snapIns := progress.Inserts
			snapUpd := progress.Updates
			snapDel := progress.Deletes
			snapErrs := progress.Errors
			progMu.RUnlock()

			if err := pollLDF(ctx, server, user, pass, dbName); err != nil && err != context.Canceled {
				logf("[%s] poll ERROR: %v", dbName, err)
				setProgress(func(p *ScanProgress) { p.ErrMsg = fmt.Sprintf("poll %s: %v", dbName, err) })
			}

			appMu.RLock()
			st := appStore
			appMu.RUnlock()
			if st != nil {
				st.Flush()
			}

			checkpointMu.RLock()
			lsnAfter := ldfCheckpoint[dbName]
			checkpointMu.RUnlock()

			progMu.RLock()
			delta := progress.Total - snapTotal
			deltaIns := progress.Inserts - snapIns
			deltaUpd := progress.Updates - snapUpd
			deltaDel := progress.Deletes - snapDel
			errDelta := progress.Errors - snapErrs
			progMu.RUnlock()

			if delta > 0 {
				logf("[%s] +%s imported (ins:%s upd:%s del:%s) · %d errors · LSN %s→%s",
					dbName, fmtN(delta), fmtN(deltaIns), fmtN(deltaUpd), fmtN(deltaDel),
					errDelta, shortLSN(lsnBefore), shortLSN(lsnAfter))
			} else {
				logf("[%s] poll scanned LSN %s→%s · 0 events · %d errors",
					dbName, shortLSN(lsnBefore), shortLSN(lsnAfter), errDelta)
			}
		}
	}
}

func pollLDF(ctx context.Context, server, user, pass, dbName string) error {
	srcDB, err := openConnection(server, user, pass, dbName)
	if err != nil {
		return err
	}
	defer srcDB.Close()

	// Use persistent state so transactions whose data records arrive in this
	// cycle but whose COMMIT arrives in the next cycle are not lost.
	state := getPollState(dbName)
	pruneStalePollState(state)

	appMu.RLock()
	sch := appSchemas[dbName]
	st := appStore
	appMu.RUnlock()
	if sch == nil || st == nil {
		return nil
	}

	if version, versionErr := readSchemaVersion(srcDB); versionErr == nil &&
		(state.schemaVersion == "" || version != state.schemaVersion) {
		refreshed, refreshErr := schema.Extract(srcDB)
		if refreshErr != nil {
			return fmt.Errorf("refresh schema: %w", refreshErr)
		}
		oldCount := len(sch.Tables)
		sch = refreshed
		state.schemaVersion = version
		appMu.Lock()
		appSchemas[dbName] = refreshed
		appMu.Unlock()
		logf("[%s] schema changed: %d→%d tables; metadata refreshed",
			dbName, oldCount, len(refreshed.Tables))
	}

	checkpointMu.RLock()
	startLSN := ldfCheckpoint[dbName]
	checkpointMu.RUnlock()

	pageReader := mssql.NewSQLPageReader(srcDB, dbName)
	gen := dml.NewWithPageReader(sch, pageReader)
	var maxLSN string
	refreshedUnknownStorage := false

	handler := func(rec *logparser.LogRecord) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		if rec.IsDataOp() &&
			sch.LookupStorage(rec.AllocUnitName, rec.AllocUnitID, rec.PartitionID) == nil &&
			!sch.IsKnownNonBaseIndex(rec.AllocUnitName) &&
			!refreshedUnknownStorage {
			refreshedUnknownStorage = true
			refreshed, refreshErr := schema.Extract(srcDB)
			if refreshErr == nil {
				sch = refreshed
				gen = dml.NewWithPageReader(refreshed, pageReader)
				appMu.Lock()
				appSchemas[dbName] = refreshed
				appMu.Unlock()
				if version, versionErr := readSchemaVersion(srcDB); versionErr == nil {
					state.schemaVersion = version
				}
				logf("[%s] unknown allocation %q; schema metadata refreshed",
					dbName, rec.AllocUnitName)
			}
		}
		return handleScanRecord(rec, gen, dbName, time.Time{}, st, state.pending, state.beginTimes, state.excluded, true, func(string) bool { return true })
	}

	r := logparser.NewLDFReader(srcDB)
	if startLSN != "" {
		r = r.WithStartLSN(startLSN)
	}
	// clearCheckpoint: set when gap-fill fails, meaning TRN files are gone and
	// LSN-based checkpointing is unusable for this DB. Subsequent polls use NULL.
	var clearCheckpoint bool

	if err := r.Read(handler); err != nil && err != context.Canceled {
		if startLSN != "" && strings.Contains(err.Error(), "Invalid parameter") {
			logf("[%s] checkpoint LSN stale — filling gap from TRN backups", dbName)
			logCheckpointLocation(srcDB, dbName, startLSN)
			n, gapErr := fillGapFromBackups(ctx, srcDB, dbName, startLSN, handler)
			if gapErr != nil {
				logf("[%s] gap-fill failed (%v) — disabling LSN checkpoint; polling from NULL (ON CONFLICT handles duplicates)", dbName, gapErr)
				clearCheckpoint = true
			} else if n == 0 {
				logf("[%s] no TRN backups found for gap — events in gap may be missing", dbName)
			}
			startLSN = ""
			checkpointMu.Lock()
			delete(ldfCheckpoint, dbName)
			checkpointMu.Unlock()
			r2 := logparser.NewLDFReader(srcDB)
			if err2 := r2.Read(handler); err2 != nil && err2 != context.Canceled {
				return err2
			}
		} else {
			return err
		}
	}
	if clearCheckpoint {
		// Erase persisted checkpoint so all future polls (including after restart)
		// use NULL. ON CONFLICT DO NOTHING suppresses duplicates.
		if st != nil {
			st.SaveCheckpoint(dbName, "")
		}
		return nil
	}
	if maxLSN != "" && maxLSN != startLSN {
		checkpointMu.Lock()
		ldfCheckpoint[dbName] = maxLSN
		checkpointMu.Unlock()
		if st != nil {
			st.SaveCheckpoint(dbName, maxLSN)
		}
	}
	return nil
}

// readSchemaVersion returns a cheap fingerprint for user-table metadata. Pollers
// compare it before reading fn_dblog so newly created/altered tables are decoded
// without restarting the process.
func readSchemaVersion(db *sql.DB) (string, error) {
	var version string
	err := db.QueryRow(`
		SELECT CONCAT(
			(SELECT COUNT_BIG(*) FROM sys.tables), ':',
			(SELECT COUNT_BIG(*)
			 FROM sys.columns c
			 JOIN sys.tables t ON t.object_id = c.object_id), ':',
			COALESCE(CONVERT(varchar(33),
				(SELECT MAX(modify_date) FROM sys.tables), 126), ''), ':',
			COALESCE(CONVERT(varchar(30),
				(SELECT CHECKSUM_AGG(BINARY_CHECKSUM(
					p.object_id, p.partition_id, p.data_compression))
				 FROM sys.partitions p
				 JOIN sys.tables t ON t.object_id = p.object_id
				 WHERE p.index_id IN (0, 1))), '0')
		)
	`).Scan(&version)
	return version, err
}

// handleScanRecord processes one log record in the committed-transaction pipeline.
// sinceTime.IsZero() = no time filter.
func handleScanRecord(
	rec *logparser.LogRecord,
	gen *dml.Generator,
	dbName string,
	sinceTime time.Time,
	st *store.DuckDBStore,
	pending map[string][]*dml.Statement,
	beginTimes map[string]time.Time,
	excluded map[string]bool,
	committedOnly bool,
	allowed func(string) bool,
) error {
	switch rec.Operation {
	case logparser.OpBeginXact:
		beginTimes[rec.TransactionID] = parseLogTime(rec.BeginTime)
		if isInternalTransaction(rec.TransactionName) {
			excluded[rec.TransactionID] = true
			delete(pending, rec.TransactionID)
			if st != nil {
				return st.DeleteTransaction(dbName, rec.TransactionID)
			}
		}
		return nil

	case logparser.OpCommitXact:
		if excluded[rec.TransactionID] {
			delete(pending, rec.TransactionID)
			delete(beginTimes, rec.TransactionID)
			delete(excluded, rec.TransactionID)
			return nil
		}
		bt := beginTimes[rec.TransactionID]
		commitTime := parseLogTime(rec.EndTime)
		if bt.IsZero() {
			bt = commitTime
		}
		if !sinceTime.IsZero() && !bt.IsZero() && bt.Before(sinceTime) {
			delete(pending, rec.TransactionID)
			delete(beginTimes, rec.TransactionID)
			delete(excluded, rec.TransactionID)
			return nil
		}
		for _, stmt := range pending[rec.TransactionID] {
			stmt.Timestamp = bt
			stmt.CommitTime = commitTime
			stmt.Database = dbName
			writeToStore(st, stmt, allowed)
		}
		delete(pending, rec.TransactionID)
		delete(beginTimes, rec.TransactionID)
		delete(excluded, rec.TransactionID)
		return nil

	case logparser.OpAbortXact:
		delete(pending, rec.TransactionID)
		delete(beginTimes, rec.TransactionID)
		delete(excluded, rec.TransactionID)
		return nil
	}

	if excluded[rec.TransactionID] {
		return nil
	}
	if isPhysicalCleanupDelete(rec) {
		excluded[rec.TransactionID] = true
		delete(pending, rec.TransactionID)
		if st != nil {
			return st.DeleteTransaction(dbName, rec.TransactionID)
		}
		return nil
	}

	stmt, err := gen.Generate(rec)
	if err != nil {
		return fmt.Errorf("generate %s %s: %w", rec.LSN, rec.Operation, err)
	}
	if stmt == nil {
		if st != nil && rec.IsDataOp() {
			return st.DeleteLSN(dbName, rec.LSN)
		}
		return nil
	}
	if committedOnly {
		pending[rec.TransactionID] = append(pending[rec.TransactionID], stmt)
		return nil
	}
	stmt.Database = dbName
	stmt.Timestamp = beginTimes[rec.TransactionID]
	writeToStore(st, stmt, allowed)
	return nil
}

func isInternalTransaction(name string) bool {
	name = strings.TrimSpace(name)
	switch name {
	case "ShrinkFile",
		"AllocHeapPageSimpleXactDML",
		"AllocFirstPage",
		"QDS base transaction",
		"QDS nested transaction":
		return true
	}
	return strings.HasPrefix(name, "Backup:")
}

// isPhysicalCleanupDelete recognizes the compact tombstone records emitted
// after SQL Server has already logged the logical heap DELETE. They are not row
// images and therefore must not be decoded or exposed as another user DELETE.
//
// Observed layout:
//
//	byte 0     internal cleanup marker (0x07 PAGE, 0x4E ROW)
//	bytes 1-8  zero
//	bytes 9-14 owning transaction tail
func isPhysicalCleanupDelete(rec *logparser.LogRecord) bool {
	if rec.Operation != logparser.OpDeleteRows || len(rec.Contents0) != 15 {
		return false
	}
	if rec.Contents0[0] != 0x07 && rec.Contents0[0] != 0x4E {
		return false
	}
	for _, b := range rec.Contents0[1:9] {
		if b != 0 {
			return false
		}
	}
	return true
}

func scanOps() []string {
	return []string{
		logparser.OpBeginXact,
		logparser.OpInsertRows, logparser.OpDeleteRows,
		logparser.OpModifyRow, logparser.OpModifyColumns,
		logparser.OpCommitXact, logparser.OpAbortXact,
	}
}

// ── Time helpers ──────────────────────────────────────────────────────────────

func parseSince(s string) (time.Time, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "all" {
		return time.Time{}, nil
	}
	if s == "now" {
		return time.Now(), nil
	}
	if strings.HasSuffix(s, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil || days <= 0 {
			return time.Time{}, fmt.Errorf("invalid --since %q: expected e.g. 7d", s)
		}
		return time.Now().Add(-time.Duration(days) * 24 * time.Hour), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid --since %q: use 24h, 7d, 30d, or all", s)
	}
	return time.Now().Add(-d), nil
}

// parseLogTime parses the SQL Server log timestamp "2024/01/15 10:00:01:123".
// fn_dblog uses a colon before milliseconds; Go requires a dot. We normalize.
func parseLogTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	// "YYYY/MM/DD HH:MM:SS:mmm" → "YYYY/MM/DD HH:MM:SS.mmm"
	if len(s) == 23 && s[19] == ':' {
		s = s[:19] + "." + s[20:]
	}
	if t, err := time.ParseInLocation("2006/01/02 15:04:05.000", s, time.Local); err == nil {
		return t
	}
	// Fallback: without milliseconds
	if len(s) > 19 {
		s = s[:19]
	}
	t, err := time.ParseInLocation("2006/01/02 15:04:05", s, time.Local)
	if err != nil {
		return time.Time{}
	}
	return t
}

// ── CORS ──────────────────────────────────────────────────────────────────────

func isAllowedOrigin(origin string) bool {
	switch {
	case origin == "":
		return true
	case origin == "https://rollback4sqlserver.dbaops.io":
		return true
	case origin == "https://rollback4sqlserver.pages.dev":
		return true
	case strings.HasSuffix(origin, ".rollback4sqlserver.pages.dev"):
		return true
	case strings.HasPrefix(origin, "http://localhost"):
		return true
	case strings.HasPrefix(origin, "http://127.0.0.1"):
		return true
	}
	return false
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if isAllowedOrigin(origin) && origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		// Chrome Private Network Access: allow HTTPS public pages → localhost requests.
		w.Header().Set("Access-Control-Allow-Private-Network", "true")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ── HTTP mux ──────────────────────────────────────────────────────────────────

func buildMux() http.Handler {
	mux := http.NewServeMux()
	// No embedded web UI — frontend is served via Cloudflare Pages.
	// Root returns a plain JSON health check for diagnostics.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]string{"status": "ok", "agent": "rollback4sqlserver"})
	})

	// Connection / interactive scan (for web UI usage without CLI flags)
	mux.HandleFunc("/api/connect", handleConnect)
	mux.HandleFunc("/api/backups", handleBackups)
	mux.HandleFunc("/api/start", handleStart)
	mux.HandleFunc("/api/stop", handleStop)
	mux.HandleFunc("/api/progress", handleProgress)

	// Log data
	mux.HandleFunc("/api/logs", handleLogs)         // event feed — primary endpoint
	mux.HandleFunc("/api/timeline", handleTimeline) // timeline — server-side bucket counts
	mux.HandleFunc("/api/search", handleSearch) // time-range filtered search
	mux.HandleFunc("/api/events", handleEvents) // legacy compat
	mux.HandleFunc("/api/diagnostics", handleDiagnostics)
	mux.HandleFunc("/api/stats", handleStats)
	mux.HandleFunc("/api/export", handleExport)

	// Utilities
	mux.HandleFunc("/api/browse", handleBrowse)
	mux.HandleFunc("/api/query", handleQuery)
	mux.HandleFunc("/api/reload-schema", handleReloadSchema)

	return corsMiddleware(mux)
}

// GET /api/diagnostics?db=MyDB&table=EventSource&limit=20
// Returns the newest non-ok decoder events with their captured raw log fields.
func handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		writeJSON(w, map[string]interface{}{"total": 0, "events": []struct{}{}})
		return
	}

	q := r.URL.Query()
	limit := queryInt(q.Get("limit"), 20)
	if limit < 1 {
		limit = 1
	}
	if limit > 200 {
		limit = 200
	}

	where := ` WHERE status IS NOT NULL AND status <> 'ok'`
	var args []interface{}
	if dbName := strings.TrimSpace(q.Get("db")); dbName != "" {
		where += ` AND db_name = ?`
		args = append(args, dbName)
	}
	if schemaName := strings.TrimSpace(q.Get("schema")); schemaName != "" {
		where += ` AND schema_name = ?`
		args = append(args, schemaName)
	}
	if tableName := strings.TrimSpace(q.Get("table")); tableName != "" {
		where += ` AND table_name = ?`
		args = append(args, tableName)
	}

	var total int
	s.DB().QueryRow(`SELECT count(*) FROM log_events`+where, args...).Scan(&total)
	rows, err := s.DB().Query(
		eventSelectColumns+where+
			fmt.Sprintf(` ORDER BY event_time DESC NULLS LAST, id DESC LIMIT %d`, limit),
		args...,
	)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()
	writeJSON(w, map[string]interface{}{
		"total":  total,
		"events": scanEventRows(rows),
	})
}

// ── Connection + session handlers ─────────────────────────────────────────────

// POST /api/connect  body: {server, user, password}
func handleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Server   string `json:"server"`
		User     string `json:"user"`
		Password string `json:"password"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	db, err := openConnection(req.Server, req.User, req.Password, "master")
	if err != nil {
		hint := ""
		if strings.Contains(err.Error(), "no instance matching") {
			hint = " — SQL Server Browser may be stopped. Use 'host,port' notation."
		}
		writeJSON(w, map[string]interface{}{"error": err.Error() + hint})
		return
	}
	defer db.Close()

	sessMu.Lock()
	sessServer = req.Server
	sessUser = req.User
	sessPassword = req.Password
	sessMu.Unlock()

	rows, err := db.Query(`SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name`)
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	defer rows.Close()
	var dbs []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		dbs = append(dbs, name)
	}
	writeJSON(w, map[string]interface{}{"databases": dbs})
}

// GET /api/backups?database=MyDB
func handleBackups(w http.ResponseWriter, r *http.Request) {
	database := r.URL.Query().Get("database")
	if database == "" {
		writeJSON(w, map[string]interface{}{"error": "database required"})
		return
	}
	sessMu.RLock()
	server, user, password := sessServer, sessUser, sessPassword
	sessMu.RUnlock()
	if server == "" {
		writeJSON(w, map[string]interface{}{"error": "not connected"})
		return
	}
	db, err := openConnection(server, user, password, "msdb")
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT TOP 500
			CONVERT(VARCHAR(23), bs.backup_finish_date, 126) AS finish_date,
			bmf.physical_device_name,
			CAST(bs.backup_size / 1048576.0 AS DECIMAL(10,1)) AS size_mb,
			CAST(bs.first_lsn AS VARCHAR(50)) AS first_lsn,
			CAST(bs.last_lsn  AS VARCHAR(50)) AS last_lsn
		FROM msdb.dbo.backupset bs
		JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
		WHERE bs.database_name = @database AND bs.type = 'L'
		ORDER BY bs.backup_finish_date DESC`,
		sql.Named("database", database))
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	defer rows.Close()

	type Backup struct {
		Date   string  `json:"date"`
		Path   string  `json:"path"`
		SizeMB float64 `json:"size_mb"`
		First  string  `json:"first_lsn"`
		Last   string  `json:"last_lsn"`
	}
	var backups []Backup
	for rows.Next() {
		var b Backup
		rows.Scan(&b.Date, &b.Path, &b.SizeMB, &b.First, &b.Last)
		backups = append(backups, b)
	}
	if backups == nil {
		backups = []Backup{}
	}
	writeJSON(w, map[string]interface{}{"backups": backups})
}

// POST /api/start — interactive scan trigger (when CLI flags were not provided)
func handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Server    string   `json:"server"`
		User      string   `json:"user"`
		Password  string   `json:"password"`
		Database  string   `json:"database"`
		Mode      string   `json:"mode"`
		Files     []string `json:"files"`
		Committed bool     `json:"committed"`
		Tables    []string `json:"tables"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	if req.Database == "" {
		writeJSON(w, map[string]interface{}{"error": "database required"})
		return
	}
	if req.Mode == "trn" && len(req.Files) == 0 {
		writeJSON(w, map[string]interface{}{"error": "TRN files required"})
		return
	}
	if !scanMu.TryLock() {
		writeJSON(w, map[string]interface{}{"error": "scan already running"})
		return
	}

	scanning.Store(true)
	progMu.Lock()
	progress = ScanProgress{Running: true, Message: "Starting…"}
	progMu.Unlock()

	go func() {
		defer scanMu.Unlock()
		defer scanning.Store(false)
		runScan(req.Server, req.User, req.Password, req.Database,
			req.Mode, req.Files, req.Committed, req.Tables)
	}()
	writeJSON(w, map[string]interface{}{"ok": true})
}

// POST /api/stop
func handleStop(w http.ResponseWriter, r *http.Request) {
	if bgCancel != nil {
		bgCancel()
		bgCtx, bgCancel = context.WithCancel(context.Background())
	}
	progMu.Lock()
	progress.Running = false
	progress.Done = true
	progress.Message = "Stopped by user"
	progMu.Unlock()
	writeJSON(w, map[string]interface{}{"ok": true})
}

// GET /api/progress
func handleProgress(w http.ResponseWriter, r *http.Request) {
	progMu.RLock()
	p := progress
	progMu.RUnlock()
	writeJSON(w, p)
}

// ── Log data handlers ─────────────────────────────────────────────────────────

// GET /api/logs?db=MyDB&limit=200&page=1&since=2024-01-01T00:00:00Z&until=...
// Primary timeline endpoint — ordered by event_time DESC.
func handleLogs(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		writeJSON(w, map[string]interface{}{"total": 0, "events": []struct{}{}})
		return
	}

	q := r.URL.Query()
	limit := queryInt(q.Get("limit"), 200)
	page := queryInt(q.Get("page"), 1)
	if page < 1 {
		page = 1
	}
	off := (page - 1) * limit

	where, args := buildWhere(q.Get("op"), q.Get("db"), q.Get("schema"), q.Get("table"), q.Get("search"), q.Get("since"), q.Get("until"))
	conn := s.DB()

	var total int
	conn.QueryRow("SELECT count(*) FROM log_events"+where, args...).Scan(&total)

	orderBy := buildOrderBy(q.Get("sort"), q.Get("dir"))
	rows, err := conn.Query(
		eventSelectColumns+
			where+orderBy+fmt.Sprintf(" LIMIT %d OFFSET %d", limit, off),
		args...)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	events := scanEventRows(rows)
	writeJSON(w, map[string]interface{}{"total": total, "page": page, "limit": limit, "events": events})
}

// GET /api/timeline?since=...&until=...&buckets=72&db=&table=&op=&search=
// Returns per-bucket INSERT/UPDATE/DELETE counts aggregated server-side with
// GROUP BY — counts ALL matching events (no row limit), unlike /api/logs.
func handleTimeline(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		writeJSON(w, map[string]interface{}{"buckets": 0, "data": []struct{}{}})
		return
	}

	q := r.URL.Query()
	buckets := queryInt(q.Get("buckets"), 72)
	if buckets < 1 || buckets > 2000 {
		buckets = 72
	}
	// Default to the last 12h when since/until are missing or unparseable,
	// so a malformed client request degrades to a sane window instead of 400/empty.
	until, err2 := time.Parse(time.RFC3339, q.Get("until"))
	if err2 != nil {
		until = time.Now().UTC()
	}
	since, err1 := time.Parse(time.RFC3339, q.Get("since"))
	if err1 != nil || !until.After(since) {
		since = until.Add(-12 * time.Hour)
	}
	sinceMs := since.UnixMilli()
	untilMs := until.UnixMilli()
	bucketMs := (untilMs - sinceMs) / int64(buckets)
	if bucketMs < 1 {
		bucketMs = 1
	}

	// Reuse the shared filter builder (db/table/op/search + since/until window).
	where, args := buildWhere(q.Get("op"), q.Get("db"), q.Get("schema"), q.Get("table"),
		q.Get("search"), since.UTC().Format(time.RFC3339), until.UTC().Format(time.RFC3339))
	conn := s.DB()

	// sinceMs/bucketMs are integers derived from parsed timestamps — safe to inline.
	sql := fmt.Sprintf(
		`SELECT CAST(floor((epoch_ms(event_time) - %d) / %d) AS BIGINT) AS b,
		        operation, count(*) AS c
		 FROM log_events%s
		 GROUP BY b, operation`,
		sinceMs, bucketMs, where)
	rows, err := conn.Query(sql, args...)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	type bucket struct {
		Insert int `json:"insert"`
		Update int `json:"update"`
		Delete int `json:"delete"`
		Total  int `json:"total"`
	}
	data := make([]bucket, buckets)
	for rows.Next() {
		var b int64
		var op string
		var c int
		if err := rows.Scan(&b, &op, &c); err != nil {
			continue
		}
		if b < 0 || b >= int64(buckets) {
			continue
		}
		switch op {
		case "INSERT":
			data[b].Insert += c
		case "UPDATE":
			data[b].Update += c
		case "DELETE":
			data[b].Delete += c
		}
		data[b].Total += c
	}
	writeJSON(w, map[string]interface{}{
		"since":     sinceMs,
		"until":     untilMs,
		"bucket_ms": bucketMs,
		"buckets":   buckets,
		"data":      data,
	})
}

// GET /api/search?db=MyDB&table=Orders&schema=dbo&op=DELETE&from=...&to=...&q=text&limit=100&offset=0
func handleSearch(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		writeJSON(w, map[string]interface{}{"total": 0, "results": []struct{}{}})
		return
	}

	q := r.URL.Query()
	limit := queryInt(q.Get("limit"), 100)
	off := queryInt(q.Get("offset"), 0)

	where, args := buildWhere(
		q.Get("op"), q.Get("db"), q.Get("schema"), q.Get("table"),
		q.Get("q"), q.Get("from"), q.Get("to"),
	)
	conn := s.DB()

	var total int
	conn.QueryRow("SELECT count(*) FROM log_events"+where, args...).Scan(&total)

	rows, err := conn.Query(
		eventSelectColumns+
			where+fmt.Sprintf(" ORDER BY event_time DESC NULLS LAST, id DESC LIMIT %d OFFSET %d", limit, off),
		args...)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	results := scanEventRows(rows)
	writeJSON(w, map[string]interface{}{"total": total, "results": results})
}

// GET /api/events?op=INSERT&schema=dbo&table=Orders&db=MyDB&q=text&limit=100&offset=0
// Legacy endpoint — ordered by id ASC.
func handleEvents(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		writeJSON(w, map[string]interface{}{"total": 0, "events": []struct{}{}})
		return
	}

	q := r.URL.Query()
	limit := queryInt(q.Get("limit"), 100)
	off := queryInt(q.Get("offset"), 0)
	where, args := buildWhere(q.Get("op"), q.Get("db"), q.Get("schema"), q.Get("table"), q.Get("q"), "", "")
	conn := s.DB()

	var total int
	conn.QueryRow("SELECT count(*) FROM log_events"+where, args...).Scan(&total)

	rows, err := conn.Query(
		eventSelectColumns+
			where+fmt.Sprintf(" ORDER BY id LIMIT %d OFFSET %d", limit, off),
		args...)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	events := scanEventRows(rows)
	writeJSON(w, map[string]interface{}{"total": total, "events": events})
}

type logEvent struct {
	LSN                     string          `json:"lsn"`
	TxnID                   string          `json:"txn_id"`
	Operation               string          `json:"operation"`
	Database                string          `json:"db_name"`
	SchemaName              string          `json:"schema_name"`
	Table                   string          `json:"table_name"`
	SQL                     string          `json:"sql_stmt"`
	RollbackSQL             string          `json:"rollback_sql,omitempty"`
	EventTime               *string         `json:"event_time,omitempty"`  // transaction begin time
	CommitTime              *string         `json:"commit_time,omitempty"` // transaction commit time
	Status                  string          `json:"status,omitempty"`
	Confidence              string          `json:"confidence,omitempty"`
	CompressionType         string          `json:"compression_type,omitempty"`
	CompressedRowHex        string          `json:"compressed_row_hex,omitempty"`
	DecompressedDebugJSON   json.RawMessage `json:"decompressed_debug_json,omitempty"`
	CompressionDecodeStatus string          `json:"compression_decode_status,omitempty"`
}

const eventSelectColumns = "SELECT lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time, commit_time, status, confidence, compression_type, compressed_row_hex, CAST(decompressed_debug_json AS VARCHAR), compression_decode_status FROM log_events"

func scanEventRows(rows *sql.Rows) []logEvent {
	var out []logEvent
	for rows.Next() {
		var e logEvent
		var rb, ts, ct, status, confidence, compressionType, compressedHex, debugJSON, decodeStatus sql.NullString
		if err := rows.Scan(
			&e.LSN, &e.TxnID, &e.Operation, &e.Database, &e.SchemaName, &e.Table, &e.SQL,
			&rb, &ts, &ct, &status, &confidence, &compressionType, &compressedHex, &debugJSON, &decodeStatus,
		); err != nil {
			logf("event row scan error: %v", err)
			continue
		}
		if rb.Valid {
			e.RollbackSQL = rb.String
		}
		if ts.Valid && ts.String != "" {
			e.EventTime = &ts.String
		}
		if ct.Valid && ct.String != "" {
			e.CommitTime = &ct.String
		}
		if status.Valid {
			e.Status = status.String
		}
		if confidence.Valid {
			e.Confidence = confidence.String
		}
		if compressionType.Valid {
			e.CompressionType = compressionType.String
		}
		if compressedHex.Valid {
			e.CompressedRowHex = compressedHex.String
		}
		if debugJSON.Valid && json.Valid([]byte(debugJSON.String)) {
			e.DecompressedDebugJSON = json.RawMessage(debugJSON.String)
		}
		if decodeStatus.Valid {
			e.CompressionDecodeStatus = decodeStatus.String
		}
		out = append(out, e)
	}
	if out == nil {
		out = []logEvent{}
	}
	return out
}

// GET /api/stats
// Returns {total, inserts, updates, deletes, tables:[], databases:[]}
func handleStats(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()

	empty := map[string]interface{}{
		"total": 0, "inserts": 0, "updates": 0, "deletes": 0,
		"tables": []string{}, "databases": []string{},
	}
	if s == nil {
		writeJSON(w, empty)
		return
	}
	conn := s.DB()

	opRows, _ := conn.Query(`SELECT operation, count(*) FROM log_events GROUP BY operation`)
	counts := map[string]int{}
	total := 0
	if opRows != nil {
		defer opRows.Close()
		for opRows.Next() {
			var op string
			var cnt int
			opRows.Scan(&op, &cnt)
			counts[op] = cnt
			total += cnt
		}
	}

	tblRows, _ := conn.Query(`SELECT DISTINCT schema_name || '.' || table_name AS t FROM log_events ORDER BY t`)
	var tableList []string
	if tblRows != nil {
		defer tblRows.Close()
		for tblRows.Next() {
			var t string
			tblRows.Scan(&t)
			tableList = append(tableList, t)
		}
	}
	if tableList == nil {
		tableList = []string{}
	}

	dbRows, _ := conn.Query(`SELECT DISTINCT db_name FROM log_events WHERE db_name IS NOT NULL AND db_name != '' ORDER BY db_name`)
	var dbList []string
	if dbRows != nil {
		defer dbRows.Close()
		for dbRows.Next() {
			var d string
			dbRows.Scan(&d)
			dbList = append(dbList, d)
		}
	}
	if dbList == nil {
		dbList = []string{}
	}

	writeJSON(w, map[string]interface{}{
		"total":     total,
		"inserts":   counts["INSERT"],
		"updates":   counts["UPDATE"],
		"deletes":   counts["DELETE"],
		"tables":    tableList,
		"databases": dbList,
	})
}

// GET /api/browse?path=C:\SQLBackups
func handleBrowse(w http.ResponseWriter, r *http.Request) {
	dir := r.URL.Query().Get("path")
	if dir == "" {
		if runtime.GOOS == "windows" {
			dir = `C:\`
		} else {
			dir, _ = os.UserHomeDir()
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error(), "path": dir})
		return
	}
	type Entry struct {
		Name  string `json:"name"`
		IsDir bool   `json:"is_dir"`
		Path  string `json:"path"`
	}
	var result []Entry
	parent := filepath.Dir(dir)
	if parent != dir {
		result = append(result, Entry{Name: "..", IsDir: true, Path: parent})
	}
	for _, e := range entries {
		name := e.Name()
		lower := strings.ToLower(name)
		isDir := e.IsDir()
		if !isDir && !strings.HasSuffix(lower, ".trn") && !strings.HasSuffix(lower, ".bak") {
			continue
		}
		result = append(result, Entry{Name: name, IsDir: isDir, Path: filepath.Join(dir, name)})
	}
	writeJSON(w, map[string]interface{}{"path": dir, "entries": result})
}

// POST /api/reload-schema — re-extracts schema from SQL Server (picks up new tables).
func handleReloadSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	sessMu.RLock()
	server, user, password := sessServer, sessUser, sessPassword
	sessMu.RUnlock()
	if server == "" {
		writeJSON(w, map[string]interface{}{"error": "not connected"})
		return
	}
	appMu.RLock()
	dbs := make([]string, 0, len(appSchemas))
	for db := range appSchemas {
		dbs = append(dbs, db)
	}
	appMu.RUnlock()
	if len(dbs) == 0 {
		writeJSON(w, map[string]interface{}{"error": "no databases scanned yet"})
		return
	}
	refreshed := map[string]int{}
	for _, dbName := range dbs {
		srcDB, err := openConnection(server, user, password, dbName)
		if err != nil {
			writeJSON(w, map[string]interface{}{"error": fmt.Sprintf("%s: %v", dbName, err)})
			return
		}
		sch, err := schema.Extract(srcDB)
		srcDB.Close()
		if err != nil {
			writeJSON(w, map[string]interface{}{"error": fmt.Sprintf("%s schema: %v", dbName, err)})
			return
		}
		appMu.Lock()
		appSchemas[dbName] = sch
		appMu.Unlock()
		refreshed[dbName] = len(sch.Tables)
		logf("[%s] schema reloaded: %d tables", dbName, len(sch.Tables))
	}
	writeJSON(w, map[string]interface{}{"ok": true, "tables": refreshed})
}

// POST /api/query  body: {"sql": "SELECT ..."}
func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		writeJSON(w, map[string]interface{}{"error": "no data yet"})
		return
	}
	var req struct {
		SQL string `json:"sql"`
	}
	json.NewDecoder(r.Body).Decode(&req)

	upper := strings.ToUpper(strings.TrimSpace(req.SQL))
	for _, kw := range []string{"INSERT ", "UPDATE ", "DELETE ", "DROP ", "CREATE ", "ALTER ", "TRUNCATE "} {
		if strings.HasPrefix(upper, kw) {
			writeJSON(w, map[string]interface{}{"error": "write statements not allowed"})
			return
		}
	}
	rows, err := s.DB().Query(req.SQL)
	if err != nil {
		writeJSON(w, map[string]interface{}{"error": err.Error()})
		return
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		row := make([]interface{}, len(cols))
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		result = append(result, row)
	}
	writeJSON(w, map[string]interface{}{"columns": cols, "rows": result})
}

// GET /api/export — full NDJSON download
func handleExport(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()
	if s == nil {
		http.Error(w, "no data", 404)
		return
	}
	rows, err := s.DB().Query(
		eventSelectColumns + ` ORDER BY id`)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	ts := time.Now().Format("20060102_150405")
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="events_%s.ndjson"`, ts))

	enc := json.NewEncoder(w)
	for rows.Next() {
		var lsn, txn, op, dbName, schName, tblName, sqlStr string
		var rollbackSQL, eventTime, commitTime, status, confidence, compressionType, compressedHex, debugJSON, decodeStatus sql.NullString
		rows.Scan(
			&lsn, &txn, &op, &dbName, &schName, &tblName, &sqlStr,
			&rollbackSQL, &eventTime, &commitTime, &status, &confidence,
			&compressionType, &compressedHex, &debugJSON, &decodeStatus,
		)
		rec := map[string]string{
			"lsn": lsn, "txn_id": txn, "operation": op,
			"db": dbName, "schema": schName, "table": tblName, "sql": sqlStr,
		}
		if rollbackSQL.Valid {
			rec["rollback_sql"] = rollbackSQL.String
		}
		if eventTime.Valid {
			rec["event_time"] = eventTime.String
		}
		if commitTime.Valid {
			rec["commit_time"] = commitTime.String
		}
		if status.Valid {
			rec["status"] = status.String
		}
		if confidence.Valid {
			rec["confidence"] = confidence.String
		}
		if compressionType.Valid {
			rec["compression_type"] = compressionType.String
		}
		if compressedHex.Valid {
			rec["compressed_row_hex"] = compressedHex.String
		}
		if debugJSON.Valid {
			rec["decompressed_debug_json"] = debugJSON.String
		}
		if decodeStatus.Valid {
			rec["compression_decode_status"] = decodeStatus.String
		}
		enc.Encode(rec)
	}
}

// ── Interactive scan goroutine (for /api/start) ───────────────────────────────

func runScan(server, user, password, database, mode string, files []string, committed bool, tables []string) {
	setProgress(func(p *ScanProgress) { p.Message = "Connecting to SQL Server…" })

	srcDB, err := openConnection(server, user, password, database)
	if err != nil {
		setProgress(func(p *ScanProgress) { p.Running = false; p.Done = true; p.ErrMsg = err.Error() })
		return
	}
	defer srcDB.Close()

	setProgress(func(p *ScanProgress) { p.Message = "Extracting schema…" })
	sch, err := schema.Extract(srcDB)
	if err != nil {
		setProgress(func(p *ScanProgress) { p.Running = false; p.Done = true; p.ErrMsg = "schema: " + err.Error() })
		return
	}
	appMu.Lock()
	appSchemas[database] = sch
	appMu.Unlock()

	appMu.RLock()
	st := appStore
	appMu.RUnlock()
	if err := st.Reset(); err != nil {
		setProgress(func(p *ScanProgress) { p.Running = false; p.Done = true; p.ErrMsg = "reset: " + err.Error() })
		return
	}

	setProgress(func(p *ScanProgress) { p.Message = "Reading log records…" })

	filterSet := make(map[string]bool, len(tables))
	for _, t := range tables {
		filterSet[strings.ToLower(t)] = true
	}
	tableAllowed := func(name string) bool {
		return len(filterSet) == 0 || filterSet[strings.ToLower(name)]
	}

	gen := dml.NewWithPageReader(sch, mssql.NewSQLPageReader(srcDB, database))
	pending := make(map[string][]*dml.Statement)
	beginTimes := make(map[string]time.Time)
	excluded := make(map[string]bool)

	handler := func(rec *logparser.LogRecord) error {
		return handleScanRecord(rec, gen, database, time.Time{}, st, pending, beginTimes, excluded, committed, tableAllowed)
	}

	var scanErr error
	switch mode {
	case "ldf", "live":
		setProgress(func(p *ScanProgress) { p.Message = "Reading live log (fn_dblog)…" })
		scanErr = logparser.NewLDFReader(srcDB).Read(handler)
	default:
		setProgress(func(p *ScanProgress) { p.Message = fmt.Sprintf("Reading %d TRN file(s)…", len(files)) })
		scanErr = logparser.NewTRNReader(srcDB, files).Read(scanOps(), handler)
	}

	st.Flush()

	progMu.Lock()
	progress.Running = false
	progress.Done = true
	if scanErr != nil {
		progress.ErrMsg = scanErr.Error()
		progress.Message = "Finished with error"
	} else {
		progress.Message = fmt.Sprintf("Done — %d events", progress.Total)
	}
	progMu.Unlock()
}

func writeToStore(st *store.DuckDBStore, stmt *dml.Statement, allowed func(string) bool) {
	if !allowed(stmt.Table) {
		return
	}
	if err := st.Write(stmt); err != nil {
		progMu.Lock()
		progress.Errors++
		progMu.Unlock()
		return
	}
	progMu.Lock()
	progress.Total++
	switch stmt.Operation {
	case "INSERT":
		progress.Inserts++
	case "UPDATE":
		progress.Updates++
	case "DELETE":
		progress.Deletes++
	default:
		progress.Other++
	}
	progMu.Unlock()
}

func setProgress(fn func(*ScanProgress)) {
	progMu.Lock()
	fn(&progress)
	progMu.Unlock()
}

// ── SQL query builder ─────────────────────────────────────────────────────────

// buildWhere constructs a parameterized WHERE clause for log_events.
// since/until are ISO 8601 strings filtering event_time.
func buildWhere(op, db, sch, tbl, search, since, until string) (string, []interface{}) {
	var conds []string
	var args []interface{}
	if op != "" {
		ops := strings.Split(op, ",")
		if len(ops) == 1 {
			conds = append(conds, "operation = ?")
			args = append(args, strings.TrimSpace(ops[0]))
		} else {
			placeholders := make([]string, len(ops))
			for i, o := range ops {
				placeholders[i] = "?"
				args = append(args, strings.TrimSpace(o))
			}
			conds = append(conds, "operation IN ("+strings.Join(placeholders, ",")+")")
		}
	}
	if db != "" {
		conds = append(conds, "db_name = ?")
		args = append(args, db)
	}
	if sch != "" {
		conds = append(conds, "schema_name = ?")
		args = append(args, sch)
	}
	if tbl != "" {
		if idx := strings.IndexByte(tbl, '.'); idx >= 0 {
			conds = append(conds, "schema_name = ? AND table_name = ?")
			args = append(args, tbl[:idx], tbl[idx+1:])
		} else {
			conds = append(conds, "table_name = ?")
			args = append(args, tbl)
		}
	}
	if search != "" {
		conds = append(conds, "sql_stmt LIKE ?")
		args = append(args, "%"+search+"%")
	}
	if since != "" {
		conds = append(conds, "event_time >= CAST(? AS TIMESTAMP)")
		args = append(args, since)
	}
	if until != "" {
		conds = append(conds, "event_time <= CAST(? AS TIMESTAMP)")
		args = append(args, until)
	}
	if len(conds) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(conds, " AND "), args
}

// buildOrderBy returns a safe ORDER BY clause.
// Allowed sort columns are whitelisted to prevent SQL injection.
func buildOrderBy(col, dir string) string {
	allowed := map[string]string{
		"event_time": "event_time",
		"lsn":        "lsn",
		"operation":  "operation",
		"db_name":    "db_name",
		"table_name": "table_name",
	}
	c, ok := allowed[col]
	if !ok {
		c = "event_time"
	}
	d := "DESC"
	if strings.ToLower(dir) == "asc" {
		d = "ASC"
	}
	// Secondary sort by id ensures stable ordering within ties.
	return fmt.Sprintf(" ORDER BY %s %s NULLS LAST, id %s", c, d, d)
}

// ── Gap-fill helpers ──────────────────────────────────────────────────────────

// logCheckpointLocation queries msdb to find and log which TRN backup file(s)
// span the given checkpoint LSN, helping diagnose gap-fill failures.
func logCheckpointLocation(srcDB *sql.DB, dbName, checkpointLSN string) {
	decLSN, err := hexLSNToDecimal(checkpointLSN)
	if err != nil {
		return
	}
	rows, err := srcDB.QueryContext(context.Background(), `
		SELECT TOP 3
			bmf.physical_device_name,
			CONVERT(VARCHAR(23), bs.backup_start_date, 120) AS started,
			CAST(bs.first_lsn AS VARCHAR(50)) AS first_lsn,
			CAST(bs.last_lsn  AS VARCHAR(50)) AS last_lsn
		FROM msdb.dbo.backupset bs
		JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
		WHERE bs.database_name = @db
		  AND bs.type = 'L'
		  AND bs.first_lsn <= CONVERT(NUMERIC(25,0), @lsn)
		  AND bs.last_lsn  >= CONVERT(NUMERIC(25,0), @lsn)
		ORDER BY bs.backup_start_date DESC`,
		sql.Named("db", dbName),
		sql.Named("lsn", decLSN))
	if err != nil {
		logf("[%s] LSN location query error: %v", dbName, err)
		return
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var path, started, firstLSN, lastLSN string
		rows.Scan(&path, &started, &firstLSN, &lastLSN)
		logf("[%s] LSN %s is in backup: %s (taken %s)", dbName, shortLSN(checkpointLSN), path, started)
		found = true
	}
	if !found {
		logf("[%s] LSN %s not found in any backup in msdb — file may be deleted or LSN too recent", dbName, shortLSN(checkpointLSN))
	}
}

// hexLSNToDecimal converts a fn_dblog hex LSN "VVVVVVVV:BBBBBBBB:SSSS" to the
// NUMERIC(25,0) decimal string stored in msdb.dbo.backupset first_lsn/last_lsn.
// SQL Server encodes the 10-byte LSN as: vlf * 2^48 + block * 2^16 + slot.
func hexLSNToDecimal(hexLSN string) (string, error) {
	parts := strings.SplitN(hexLSN, ":", 3)
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid LSN: %q", hexLSN)
	}
	vlf, err1 := strconv.ParseUint(strings.TrimSpace(parts[0]), 16, 64)
	blk, err2 := strconv.ParseUint(strings.TrimSpace(parts[1]), 16, 64)
	slot, err3 := strconv.ParseUint(strings.TrimSpace(parts[2]), 16, 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return "", fmt.Errorf("invalid LSN hex: %q", hexLSN)
	}
	pow48 := new(big.Int).Lsh(big.NewInt(1), 48)
	pow16 := new(big.Int).Lsh(big.NewInt(1), 16)
	result := new(big.Int)
	result.Mul(new(big.Int).SetUint64(vlf), pow48)
	result.Add(result, new(big.Int).Mul(new(big.Int).SetUint64(blk), pow16))
	result.Add(result, new(big.Int).SetUint64(slot))
	return result.String(), nil
}

// fillGapFromBackups scans TRN log backup files that cover the LSN gap between
// checkpointLSN and the current active log start, using fn_dump_dblog.
// Returns the number of backup files found and scanned; 0 means no backups
// covered the gap and events in the gap are lost.
func fillGapFromBackups(ctx context.Context, srcDB *sql.DB, dbName, checkpointLSN string, handler func(*logparser.LogRecord) error) (int, error) {
	decLSN, err := hexLSNToDecimal(checkpointLSN)
	if err != nil {
		return 0, fmt.Errorf("LSN convert: %w", err)
	}

	// Query msdb for TRN backups that cover the gap. Limit to the last 7 days
	// and at most 64 files (one fn_dump_dblog call) to avoid scanning stale
	// or already-deleted backup files.
	rows, err := srcDB.QueryContext(ctx, `
		SELECT TOP 64 bmf.physical_device_name
		FROM msdb.dbo.backupset bs
		JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
		WHERE bs.database_name = @db
		  AND bs.type = 'L'
		  AND bs.last_lsn >= CONVERT(NUMERIC(25,0), @lsn)
		  AND bs.backup_start_date >= DATEADD(day, -7, GETDATE())
		ORDER BY bs.first_lsn`,
		sql.Named("db", dbName),
		sql.Named("lsn", decLSN))
	if err != nil {
		return 0, fmt.Errorf("msdb query: %w", err)
	}
	defer rows.Close()

	var files []string
	for rows.Next() {
		var f string
		rows.Scan(&f)
		if f != "" {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return 0, nil
	}

	logf("[%s] gap-fill: scanning %d TRN file(s) from LSN %s", dbName, len(files), shortLSN(checkpointLSN))
	r := logparser.NewTRNReader(srcDB, files).WithLSNRange(checkpointLSN, "")
	if err := r.Read(scanOps(), handler); err != nil && err != context.Canceled {
		return len(files), fmt.Errorf("gap-fill scan: %w", err)
	}
	return len(files), nil
}

// ── Connection helpers ────────────────────────────────────────────────────────

func buildServerAddr(host string, sqlPort int) string {
	if sqlPort != 0 && sqlPort != 1433 {
		return fmt.Sprintf("%s,%d", host, sqlPort)
	}
	return host
}

func openConnection(server, user, password, database string) (*sql.DB, error) {
	dsn := buildDSN(server, user, password, database)
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	if err2 := db.Ping(); err2 == nil {
		return db, nil
	} else {
		db.Close()
		// Fall back to port 1433 if Browser discovery fails for a named instance.
		if strings.Contains(err2.Error(), "no instance matching") {
			fallback := buildDSNPort(server, user, password, database, 1433)
			db2, _ := sql.Open("sqlserver", fallback)
			if db2 != nil {
				if err3 := db2.Ping(); err3 == nil {
					return db2, nil
				}
				db2.Close()
			}
		}
		return nil, err2
	}
}

func buildDSN(server, user, password, database string) string {
	// Normalize "host,port" → "host:port" for URL-style DSN.
	serverNorm := strings.Replace(server, ",", ":", 1)

	host := serverNorm
	instance := ""
	if idx := strings.IndexByte(serverNorm, '\\'); idx >= 0 {
		host = serverNorm[:idx]
		instance = serverNorm[idx+1:]
	} else if idx := strings.IndexByte(serverNorm, '/'); idx >= 0 {
		host = serverNorm[:idx]
		instance = serverNorm[idx+1:]
	}
	switch host {
	case ".", "(local)", "":
		host = "localhost"
	}

	var u url.URL
	u.Scheme = "sqlserver"
	u.Host = host
	if instance != "" {
		u.Path = "/" + instance
	}
	if user != "" {
		u.User = url.UserPassword(user, password)
	}
	q := url.Values{}
	if database != "" {
		q.Set("database", database)
	}
	if user == "" {
		q.Set("trusted_connection", "yes")
	}
	q.Set("TrustServerCertificate", "true")
	q.Set("dial timeout", "5")
	u.RawQuery = q.Encode()
	return u.String()
}

func buildDSNPort(server, user, password, database string, port int) string {
	host := server
	for _, sep := range []string{"\\", "/", ","} {
		if idx := strings.Index(server, sep); idx >= 0 {
			host = server[:idx]
			break
		}
	}
	switch host {
	case ".", "(local)", "":
		host = "localhost"
	}
	var u url.URL
	u.Scheme = "sqlserver"
	u.Host = fmt.Sprintf("%s:%d", host, port)
	if user != "" {
		u.User = url.UserPassword(user, password)
	}
	q := url.Values{}
	if database != "" {
		q.Set("database", database)
	}
	if user == "" {
		q.Set("trusted_connection", "yes")
	}
	q.Set("TrustServerCertificate", "true")
	q.Set("dial timeout", "5")
	u.RawQuery = q.Encode()
	return u.String()
}

// ── Misc helpers ──────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func queryInt(s string, def int) int {
	var n int
	if _, err := fmt.Sscan(s, &n); err != nil || n <= 0 {
		return def
	}
	return n
}
