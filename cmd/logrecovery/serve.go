package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"

	"github.com/uns/mssqllogrecovery/internal/dml"
	"github.com/uns/mssqllogrecovery/internal/logparser"
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
	appMu    sync.RWMutex
	appStore *store.DuckDBStore
	appSch   *schema.Schema

	scanMu   sync.Mutex
	scanning atomic.Bool
	progress ScanProgress
	progMu   sync.RWMutex

	sessMu                             sync.RWMutex
	sessServer, sessUser, sessPassword string

	// LSN checkpoint per database for incremental LDF polling.
	checkpointMu  sync.RWMutex
	ldfCheckpoint = map[string]string{}

	// Context controlling all auto-scan / poller goroutines.
	bgCtx    context.Context
	bgCancel context.CancelFunc
)

// ── serve command ─────────────────────────────────────────────────────────────

func serveCmd() *cobra.Command {
	var (
		httpPort int
		host     string
		user     string
		pass     string
		sqlPort  int
		dbName   string
		allDBs   bool
		since    string
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the local agent (REST API + web UI)",
		Long: `Starts the privacy-first local log-recovery agent.

If --host (plus --db or --all-dbs) is given, the agent auto-connects on
startup, loads history bounded by --since, then continuously polls the live
transaction log every 5 s — no manual interaction required.

The REST API at http://localhost:<port>/api/ is designed to be called directly
by the browser from https://rollback4sqlserver.dbaops.io.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sinceTime, err := parseSince(since)
			if err != nil {
				return err
			}
			return runServe(httpPort, host, user, pass, sqlPort, dbName, allDBs, sinceTime)
		},
	}
	cmd.Flags().IntVar(&httpPort, "port", 8182, "HTTP port for the local agent")
	cmd.Flags().StringVar(&host, "host", "", "SQL Server host (triggers auto-scan)")
	cmd.Flags().StringVar(&user, "user", "", "SQL Server login (blank = Windows auth)")
	cmd.Flags().StringVar(&pass, "pass", "", "SQL Server password")
	cmd.Flags().IntVar(&sqlPort, "sql-port", 1433, "SQL Server TCP port")
	cmd.Flags().StringVar(&dbName, "db", "", "Database to scan")
	cmd.Flags().BoolVar(&allDBs, "all-dbs", false, "Scan all online user databases automatically")
	cmd.Flags().StringVar(&since, "since", "24h", "History depth: 24h | 7d | 30d | all")
	return cmd
}

func runServe(httpPort int, host, user, pass string, sqlPort int, dbName string, allDBs bool, sinceTime time.Time) error {
	bgCtx, bgCancel = context.WithCancel(context.Background())

	dbPath := store.PersistentDBPath() // "" = in-memory, else persistent file
	var err error
	appStore, err = store.OpenDuckDB(dbPath)
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}
	if dbPath != "" {
		// Persistent mode: start background TTL worker to enforce 30-day retention.
		appStore.StartTTLWorker(bgCtx)
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
		}
	}

	addr := fmt.Sprintf(":%d", httpPort)
	fmt.Printf("\n  MSSQL Log Recovery — Local Agent\n")
	fmt.Printf("  API  : http://localhost%s/api/\n", addr)
	fmt.Printf("  UI   : https://rollback4sqlserver.dbaops.io  → connect to http://localhost%s\n\n", addr)
	return http.ListenAndServe(addr, buildMux())
}

// ── Auto-scan orchestration ───────────────────────────────────────────────────

func runAutoScan(server, user, pass, dbName string, allDBs bool, sinceTime time.Time) {
	setProgress(func(p *ScanProgress) { p.Running = true; p.Message = "Resolving target databases…" })

	dbs, err := resolveTargetDBs(server, user, pass, dbName, allDBs)
	if err != nil {
		setProgress(func(p *ScanProgress) { p.Running = false; p.Done = true; p.ErrMsg = err.Error() })
		return
	}

	for _, db := range dbs {
		select {
		case <-bgCtx.Done():
			return
		default:
		}
		setProgress(func(p *ScanProgress) { p.Message = fmt.Sprintf("Initial scan: %s…", db) })
		if err := scanDatabase(bgCtx, server, user, pass, db, sinceTime); err != nil && err != context.Canceled {
			setProgress(func(p *ScanProgress) { p.ErrMsg = fmt.Sprintf("%s: %v", db, err) })
		}
	}

	appMu.RLock()
	st := appStore
	appMu.RUnlock()
	if err := st.Flush(); err != nil {
		setProgress(func(p *ScanProgress) { p.ErrMsg = "flush: " + err.Error() })
	}

	setProgress(func(p *ScanProgress) {
		p.Message = fmt.Sprintf("Polling live log every 5 s for %d database(s) — %d events loaded", len(dbs), p.Total)
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
	appMu.Lock()
	appSch = sch
	appMu.Unlock()

	appMu.RLock()
	st := appStore
	appMu.RUnlock()

	gen := dml.New(sch)
	pending := make(map[string][]*dml.Statement)
	beginTimes := make(map[string]time.Time)
	var maxLSN string

	ops := scanOps()
	handler := func(rec *logparser.LogRecord) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		return handleScanRecord(rec, gen, dbName, sinceTime, st, pending, beginTimes, true, func(string) bool { return true })
	}

	if err := logparser.NewLDFReader(srcDB).Read(ops, handler); err != nil && err != context.Canceled {
		return err
	}
	if maxLSN != "" {
		checkpointMu.Lock()
		ldfCheckpoint[dbName] = maxLSN
		checkpointMu.Unlock()
	}
	return nil
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
			if err := pollLDF(ctx, server, user, pass, dbName); err != nil && err != context.Canceled {
				setProgress(func(p *ScanProgress) { p.ErrMsg = fmt.Sprintf("poll %s: %v", dbName, err) })
			}
			appMu.RLock()
			st := appStore
			appMu.RUnlock()
			if st != nil {
				st.Flush()
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

	appMu.RLock()
	sch := appSch
	st := appStore
	appMu.RUnlock()
	if sch == nil || st == nil {
		return nil
	}

	checkpointMu.RLock()
	startLSN := ldfCheckpoint[dbName]
	checkpointMu.RUnlock()

	gen := dml.New(sch)
	pending := make(map[string][]*dml.Statement)
	beginTimes := make(map[string]time.Time)
	var maxLSN string

	handler := func(rec *logparser.LogRecord) error {
		select {
		case <-ctx.Done():
			return context.Canceled
		default:
		}
		if rec.LSN == startLSN {
			return nil // skip boundary record (already stored)
		}
		if rec.LSN > maxLSN {
			maxLSN = rec.LSN
		}
		return handleScanRecord(rec, gen, dbName, time.Time{}, st, pending, beginTimes, true, func(string) bool { return true })
	}

	r := logparser.NewLDFReader(srcDB)
	if startLSN != "" {
		r = r.WithLSNRange(startLSN, "")
	}
	if err := r.Read(scanOps(), handler); err != nil && err != context.Canceled {
		return err
	}
	if maxLSN != "" && maxLSN != startLSN {
		checkpointMu.Lock()
		ldfCheckpoint[dbName] = maxLSN
		checkpointMu.Unlock()
	}
	return nil
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
	committedOnly bool,
	allowed func(string) bool,
) error {
	switch rec.Operation {
	case logparser.OpBeginXact:
		beginTimes[rec.TransactionID] = parseLogTime(rec.BeginTime)
		return nil

	case logparser.OpCommitXact:
		bt := beginTimes[rec.TransactionID]
		if !sinceTime.IsZero() && !bt.IsZero() && bt.Before(sinceTime) {
			delete(pending, rec.TransactionID)
			delete(beginTimes, rec.TransactionID)
			return nil
		}
		for _, stmt := range pending[rec.TransactionID] {
			stmt.Timestamp = bt
			stmt.Database = dbName
			writeToStore(st, stmt, allowed)
		}
		delete(pending, rec.TransactionID)
		delete(beginTimes, rec.TransactionID)
		return nil

	case logparser.OpAbortXact:
		delete(pending, rec.TransactionID)
		delete(beginTimes, rec.TransactionID)
		return nil
	}

	stmt, err := gen.Generate(rec)
	if err != nil || stmt == nil {
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

// parseLogTime parses the SQL Server log timestamp "2024/01/15 10:00:01:000".
func parseLogTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.ParseInLocation("2006/01/02 15:04:05:000", s, time.Local)
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
	mux.HandleFunc("/api/logs", handleLogs)       // timeline — primary endpoint
	mux.HandleFunc("/api/search", handleSearch)   // time-range filtered search
	mux.HandleFunc("/api/events", handleEvents)   // legacy compat
	mux.HandleFunc("/api/stats", handleStats)
	mux.HandleFunc("/api/export", handleExport)

	// Utilities
	mux.HandleFunc("/api/browse", handleBrowse)
	mux.HandleFunc("/api/query", handleQuery)

	return corsMiddleware(mux)
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
		"SELECT lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time FROM log_events"+
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
		"SELECT lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time FROM log_events"+
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
		"SELECT lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time FROM log_events"+
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
	LSN         string  `json:"lsn"`
	TxnID       string  `json:"txn_id"`
	Operation   string  `json:"operation"`
	Database    string  `json:"db_name"`
	SchemaName  string  `json:"schema_name"`
	Table       string  `json:"table_name"`
	SQL         string  `json:"sql_stmt"`
	RollbackSQL string  `json:"rollback_sql,omitempty"`
	EventTime   *string `json:"event_time,omitempty"`
}

func scanEventRows(rows *sql.Rows) []logEvent {
	var out []logEvent
	for rows.Next() {
		var e logEvent
		var rb, ts sql.NullString
		rows.Scan(&e.LSN, &e.TxnID, &e.Operation, &e.Database, &e.SchemaName, &e.Table, &e.SQL, &rb, &ts)
		if rb.Valid {
			e.RollbackSQL = rb.String
		}
		if ts.Valid && ts.String != "" {
			e.EventTime = &ts.String
		}
		out = append(out, e)
	}
	if out == nil {
		out = []logEvent{}
	}
	return out
}

// GET /api/stats
func handleStats(w http.ResponseWriter, r *http.Request) {
	appMu.RLock()
	s := appStore
	appMu.RUnlock()

	type OpStat struct {
		Operation string `json:"operation"`
		Count     int    `json:"cnt"`
	}
	empty := map[string]interface{}{
		"ops": []OpStat{}, "tables": 0,
		"table_list": []string{}, "databases": []string{},
	}
	if s == nil {
		writeJSON(w, empty)
		return
	}
	conn := s.DB()

	opRows, _ := conn.Query(
		`SELECT operation, count(*) FROM log_events GROUP BY operation ORDER BY count(*) DESC`)
	var ops []OpStat
	if opRows != nil {
		defer opRows.Close()
		for opRows.Next() {
			var st OpStat
			opRows.Scan(&st.Operation, &st.Count)
			ops = append(ops, st)
		}
	}
	if ops == nil {
		ops = []OpStat{}
	}

	var tableCount int
	conn.QueryRow(
		`SELECT count(DISTINCT db_name || '.' || schema_name || '.' || table_name) FROM log_events`).Scan(&tableCount)

	tblRows, _ := conn.Query(
		`SELECT DISTINCT schema_name || '.' || table_name AS t FROM log_events ORDER BY t`)
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

	dbRows, _ := conn.Query(
		`SELECT DISTINCT db_name FROM log_events WHERE db_name IS NOT NULL AND db_name != '' ORDER BY db_name`)
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
		"ops": ops, "tables": tableCount,
		"table_list": tableList, "databases": dbList,
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
		`SELECT lsn, txn_id, operation, db_name, schema_name, table_name, sql_stmt, rollback_sql, event_time FROM log_events ORDER BY id`)
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
		var rollbackSQL, eventTime sql.NullString
		rows.Scan(&lsn, &txn, &op, &dbName, &schName, &tblName, &sqlStr, &rollbackSQL, &eventTime)
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
	appSch = sch
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

	gen := dml.New(sch)
	pending := make(map[string][]*dml.Statement)
	beginTimes := make(map[string]time.Time)

	handler := func(rec *logparser.LogRecord) error {
		return handleScanRecord(rec, gen, database, time.Time{}, st, pending, beginTimes, committed, tableAllowed)
	}

	var scanErr error
	switch mode {
	case "ldf", "live":
		setProgress(func(p *ScanProgress) { p.Message = "Reading live log (fn_dblog)…" })
		scanErr = logparser.NewLDFReader(srcDB).Read(scanOps(), handler)
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
		conds = append(conds, "operation = ?")
		args = append(args, op)
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

func openBrowser(u string) {
	switch runtime.GOOS {
	case "windows":
		exec.Command("cmd", "/c", "start", u).Start()
	case "darwin":
		exec.Command("open", u).Start()
	default:
		exec.Command("xdg-open", u).Start()
	}
}
