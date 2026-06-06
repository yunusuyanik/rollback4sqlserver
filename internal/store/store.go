package store

import "github.com/uns/mssqllogrecovery/internal/dml"

// Store persists generated DML statements for later querying.
type Store interface {
	Write(stmt *dml.Statement) error
	Flush() error // flush any buffered writes
	Close() error
}
