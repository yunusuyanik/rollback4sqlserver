# rollback4sqlserver

Local SQL Server transaction log rollback tool.

`rollback4sqlserver` reads SQL Server transaction log records through `fn_dblog`, shows INSERT/UPDATE/DELETE activity in a local browser UI, and generates inverse SQL you can review before running.

## Download

Download the latest build from:

https://github.com/yunusuyanik/rollback4sqlserver/releases/latest

Packages:

- Windows amd64: contains `rollback4sqlserver.exe`
- Linux amd64: contains `rollback4sqlserver`

## Run

Windows:

```powershell
.\rollback4sqlserver.exe --host ".\SQLEXPRESS" --user sa --pass "your-password" --all-dbs --since 24h
```

Linux:

```bash
./rollback4sqlserver --host "192.168.1.10" --user sa --pass "your-password" --all-dbs --since 24h
```

Then open:

```text
http://localhost:8182
```

## Options

```text
--host       SQL Server host, for example .\SQLEXPRESS or 192.168.1.10
--user       SQL Server login
--pass       SQL Server password
--sql-port   SQL Server TCP port, default 1433
--db         Scan one database
--all-dbs    Scan all online user databases
--since      24h | 7d | 30d | all | now
--port       Local HTTP port, default 8182
--data-dir   Directory for local DuckDB storage
```

## Requirements

- SQL Server 2016 or later
- SQL login with `VIEW DATABASE STATE`
- Network access from the machine running `rollback4sqlserver` to SQL Server

## Build

```bash
go test ./...
go build -ldflags="-s -w" -o rollback4sqlserver ./cmd/logrecovery
```

## License

MIT License.
