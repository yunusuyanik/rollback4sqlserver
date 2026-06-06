package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"

	"github.com/uns/mssqllogrecovery/internal/config"
	"github.com/uns/mssqllogrecovery/internal/dml"
	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/schema"
	"github.com/uns/mssqllogrecovery/internal/store"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var port int
	root := &cobra.Command{
		Use:   "logrecovery",
		Short: "MSSQL Log Recovery — web UI for reading SQL Server transaction logs",
		// No subcommand → launch web UI directly (e.g. double-click on Windows)
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(port, "", "", "", 1433, "", false, time.Time{})
		},
	}
	root.Flags().IntVar(&port, "port", 8182, "HTTP listen port")
	root.AddCommand(schemaCmd(), readCmd(), serveCmd())
	return root
}

// ── schema extract ──────────────────────────────────────────────────────────

func schemaCmd() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Extract and cache table schema from the source SQL Server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			db, err := sql.Open("sqlserver", cfg.Source.DSN)
			if err != nil {
				return fmt.Errorf("open source: %w", err)
			}
			defer db.Close()

			fmt.Fprintln(os.Stderr, "Extracting schema…")
			sch, err := schema.Extract(db)
			if err != nil {
				return err
			}

			out := cfg.SchemaCache
			if out == "" {
				out = "schema.json"
			}
			if err := schema.Save(sch, out); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "Saved %d tables to %s\n", len(sch.Tables), out)
			return nil
		},
	}
	cmd.Flags().StringVarP(&cfgPath, "config", "c", "config.yaml", "config file")
	return cmd
}

// ── read (TRN/LDF) ──────────────────────────────────────────────────────────

func readCmd() *cobra.Command {
	var (
		cfgPath     string
		startLSN    string
		endLSN      string
		tableFilter []string
		committed   bool
		dbPath      string // SQLite output (empty = stdout only)
	)
	cmd := &cobra.Command{
		Use:   "read",
		Short: "Read log records and output DML statements",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}

			// Load schema.
			cachePath := cfg.SchemaCache
			if cachePath == "" {
				cachePath = "schema.json"
			}
			sch, err := schema.Load(cachePath)
			if err != nil {
				return fmt.Errorf("load schema (run 'logrecovery schema' first): %w", err)
			}

			// Open analysis connection (used for fn_dump_dblog / fn_dblog).
			analysisDSN := cfg.Analysis.DSN
			if analysisDSN == "" {
				analysisDSN = cfg.Source.DSN // fall back to source (fn_dump_dblog is read-only)
			}
			db, err := sql.Open("sqlserver", analysisDSN)
			if err != nil {
				return fmt.Errorf("open analysis db: %w", err)
			}
			defer db.Close()

			// Output writer.
			var out io.Writer = os.Stdout
			if cfg.Output.File != "" {
				f, err := os.Create(cfg.Output.File)
				if err != nil {
					return err
				}
				defer f.Close()
				out = f
			}

			// Table filter (union of config + flag).
			filterSet := make(map[string]bool)
			for _, t := range append(cfg.Input.Tables, tableFilter...) {
				filterSet[strings.ToLower(t)] = true
			}
			tableAllowed := func(name string) bool {
				if len(filterSet) == 0 {
					return true
				}
				return filterSet[strings.ToLower(name)]
			}

			// Optional SQLite store.
			var db2 *store.SQLiteStore
			if dbPath != "" {
				var err2 error
				db2, err2 = store.Open(dbPath)
				if err2 != nil {
					return fmt.Errorf("open store: %w", err2)
				}
				defer db2.Close()
				fmt.Fprintf(os.Stderr, "Writing events to %s\n", dbPath)
			}

			gen := dml.New(sch)

			// pending holds statements for in-flight transactions (--committed mode).
			pending := make(map[string][]*dml.Statement)

			ops := []string{
				logparser.OpInsertRows,
				logparser.OpDeleteRows,
				logparser.OpModifyRow,
				logparser.OpModifyColumns,
				logparser.OpCommitXact,
				logparser.OpAbortXact,
			}

			handler := func(rec *logparser.LogRecord) error {
				if committed {
					switch rec.Operation {
					case logparser.OpCommitXact:
						for _, s := range pending[rec.TransactionID] {
							if db2 != nil {
								db2.Write(s)
							}
							if err := writeStmt(out, s, cfg.Output.Format); err != nil {
								return err
							}
						}
						delete(pending, rec.TransactionID)
						return nil
					case logparser.OpAbortXact:
						delete(pending, rec.TransactionID)
						return nil
					}
				}

				stmt, err := gen.Generate(rec)
				if err != nil {
					fmt.Fprintf(os.Stderr, "warn: %v (LSN %s)\n", err, rec.LSN)
					return nil
				}
				if stmt == nil {
					return nil
				}
				t := sch.Lookup(rec.AllocUnitName)
				if t != nil && !tableAllowed(t.Schema+"."+t.Name) {
					return nil
				}

				if committed {
					pending[rec.TransactionID] = append(pending[rec.TransactionID], stmt)
					return nil
				}
				if db2 != nil {
					if err := db2.Write(stmt); err != nil {
						fmt.Fprintf(os.Stderr, "warn: store write: %v\n", err)
					}
				}
				return writeStmt(out, stmt, cfg.Output.Format)
			}

			switch cfg.Input.Mode {
			case "ldf", "live":
				r := logparser.NewLDFReader(db).WithLSNRange(startLSN, endLSN)
				fmt.Fprintln(os.Stderr, "Reading live transaction log (fn_dblog)…")
				return r.Read(ops, handler)

			default: // "trn" is default
				if len(cfg.Input.Files) == 0 {
					return fmt.Errorf("no input files specified (set input.files in config)")
				}
				r := logparser.NewTRNReader(db, cfg.Input.Files).WithLSNRange(startLSN, endLSN)
				fmt.Fprintln(os.Stderr, "Reading TRN backup files (fn_dump_dblog)…")
				return r.Read(ops, handler)
			}
		},
	}
	cmd.Flags().StringVarP(&cfgPath, "config", "c", "config.yaml", "config file")
	cmd.Flags().StringVar(&startLSN, "start-lsn", "", "start LSN (e.g. 00000001:00000001:0001)")
	cmd.Flags().StringVar(&endLSN, "end-lsn", "", "end LSN")
	cmd.Flags().StringArrayVar(&tableFilter, "table", nil, "filter by table (schema.table), repeatable")
	cmd.Flags().BoolVar(&committed, "committed", false, "only emit DML for committed transactions")
	cmd.Flags().StringVar(&dbPath, "db", "", "write events to SQLite file (for web UI)")
	return cmd
}

func writeStmt(w io.Writer, s *dml.Statement, format string) error {
	if format == "json" {
		return json.NewEncoder(w).Encode(s)
	}
	// Default: plain SQL with comment header.
	_, err := fmt.Fprintf(w, "-- LSN: %s  TXN: %s  OP: %s  TABLE: %s\n%s\n\n",
		s.LSN, s.TransactionID, s.Operation, s.Table, s.SQL)
	return err
}
