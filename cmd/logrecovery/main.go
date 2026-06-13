package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"

	"github.com/uns/mssqllogrecovery/internal/config"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var (
		httpPort int
		host     string
		user     string
		pass     string
		sqlPort  int
		dbName   string
		allDBs   bool
		since    string
		dataDir  string
	)
	root := &cobra.Command{
		Use:   "logrecovery",
		Short: "MSSQL Log Recovery agent",
		Long: `Reads the SQL Server transaction log and exposes recovered DML events
via a local REST API at http://localhost:<port>/api/.

Open https://rollback4sqlserver.dbaops.io in your browser and connect to
http://localhost:<port> to browse and filter events.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sinceTime, err := parseSince(since)
			if err != nil {
				return err
			}
			return runServe(httpPort, host, user, pass, sqlPort, dbName, allDBs, sinceTime, dataDir)
		},
	}
	root.Flags().IntVar(&httpPort, "port", 8182, "HTTP listen port for the REST API")
	root.Flags().StringVar(&host, "host", "", "SQL Server host (e.g. 192.168.1.10 or SERVER\\INSTANCE)")
	root.Flags().StringVar(&user, "user", "", "SQL Server login (leave blank for Windows auth)")
	root.Flags().StringVar(&pass, "pass", "", "SQL Server password")
	root.Flags().IntVar(&sqlPort, "sql-port", 1433, "SQL Server TCP port")
	root.Flags().StringVar(&dbName, "db", "", "Database to scan")
	root.Flags().BoolVar(&allDBs, "all-dbs", false, "Scan all online user databases")
	root.Flags().StringVar(&since, "since", "24h", "History window: 24h | 7d | 30d | all | now")
	root.Flags().StringVar(&dataDir, "data-dir", "", "Directory for persistent DuckDB storage (default: ./data next to executable)")
	root.AddCommand(schemaCmd())
	return root
}

// ── schema extract ──────────────────────────────────────────────────────────

func schemaCmd() *cobra.Command {
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Extract and save table schema from a SQL Server instance",
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
