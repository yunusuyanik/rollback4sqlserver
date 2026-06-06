# rollback4sqlserver

**Instant SQL Server transaction log recovery and rollback — by dbaops**

Reads your SQL Server transaction log via `fn_dblog`, decodes every INSERT/UPDATE/DELETE, and generates the exact inverse SQL to undo any committed change. Privacy-first: your data never leaves your server.

---

## Features

- **Inverse SQL Engine** — INSERT → DELETE, DELETE → INSERT, UPDATE → UPDATE with before-image values
- **Live Log Monitoring** — polls `fn_dblog` every 5 seconds, zero DDL/DML on source
- **Bulk Rollback Scripts** — select multiple events, generate a complete `BEGIN TRANSACTION` block in reverse-LSN order
- **Privacy-First** — agent runs locally on your Windows server, data never sent to the cloud
- **Zero Installation** — single `.exe`, embedded DuckDB, no runtime dependencies
- **Powerful Filtering** — by database, schema, table, operation, time range, full-text search

---

## Quick Start

1. **Download** `logrecovery.exe` from [Releases](https://github.com/yunusuyanik/rollback4sqlserver/releases/latest)

2. **Run the agent** on your Windows server:
   ```
   .\logrecovery.exe serve --host ".\SQLEXPRESS" --user sa --pass *** --all-dbs --since 24h
   ```

3. **Open the UI** at [rollback4sqlserver.dbaops.io](https://rollback4sqlserver.dbaops.io) — connects to `localhost:8182`

---

## CLI Reference

```
logrecovery serve [flags]

Flags:
  --host       SQL Server host (e.g. .\SQLEXPRESS or 192.168.1.10)
  --user       SQL Server login
  --pass       SQL Server password
  --sql-port   TCP port (default 1433)
  --db         Single database to scan
  --all-dbs    Scan all online user databases
  --since      History depth: 24h | 7d | 30d | all (default: 24h)
  --port       HTTP port for local agent (default: 8182)
```

---

## Architecture

```
Browser → rollback4sqlserver.dbaops.io  (static HTML, Cloudflare CDN)
Browser → localhost:8182                (local agent, your machine)
Agent   → SQL Server fn_dblog           (read-only SELECT, no DDL/DML)
Agent   → DuckDB (in-memory)            (embedded, no external database)
```

SQL Server data **never** leaves your server. The cloud only hosts the UI shell.

---

## How It Works

`fn_dblog` / `fn_dump_dblog` are SQL Server built-in table-valued functions that expose the transaction log as a SELECT. `logrecovery` uses these read-only TVFs to:

1. Decode row images from `RowLog Contents 0/1` binary blobs
2. Correlate `LOP_INSERT_ROWS`, `LOP_DELETE_ROWS`, `LOP_MODIFY_ROW` with `LOP_COMMIT_XACT`
3. Generate forward SQL (what happened) and inverse SQL (how to undo it)
4. Store events in an embedded DuckDB instance
5. Serve results via a REST API to the browser UI

---

## Build from Source

Requires Go 1.21+ and CGO for DuckDB.

```bash
# macOS / Linux
go build -o logrecovery ./cmd/logrecovery/

# Windows cross-compile from macOS (requires mingw-w64)
brew install mingw-w64
GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CC=x86_64-w64-mingw32-gcc \
  go build -ldflags="-s -w" -o logrecovery.exe ./cmd/logrecovery/
```

---

## Requirements

- SQL Server 2016 or later
- Windows Server (for the agent exe)
- SQL login with `VIEW DATABASE STATE` permission (for `fn_dblog`)
- Any modern browser

---

## License

MIT — free for commercial and personal use.

---

*by [dbaops](https://github.com/yunusuyanik)*
