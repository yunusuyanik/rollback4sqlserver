package mssql

import (
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// CompressionType describes SQL Server data_compression_desc values.
type CompressionType string

const (
	CompressionNone        CompressionType = "NONE"
	CompressionRow         CompressionType = "ROW"
	CompressionPage        CompressionType = "PAGE"
	CompressionColumnstore CompressionType = "COLUMNSTORE"
	CompressionUnknown     CompressionType = "UNKNOWN"
)

// CompressionFromInt maps sys.partitions.data_compression to CompressionType.
func CompressionFromInt(v int) CompressionType {
	switch v {
	case schema.CompressionNone:
		return CompressionNone
	case schema.CompressionRow:
		return CompressionRow
	case schema.CompressionPage:
		return CompressionPage
	case 3, 4: // COLUMNSTORE, COLUMNSTORE_ARCHIVE
		return CompressionColumnstore
	default:
		return CompressionUnknown
	}
}

// ResolveCompression picks the best compression metadata for a log record.
// AllocUnitID is preferred; PartitionID is the fallback; table default is last resort.
func ResolveCompression(sch *schema.Schema, table *schema.Table, allocUnitID, partitionID int64) CompressionType {
	if sch == nil || table == nil {
		return CompressionUnknown
	}
	if allocUnitID > 0 {
		if meta, ok := sch.CompressionByAllocUnit[allocUnitID]; ok {
			return CompressionFromInt(meta.DataCompression)
		}
	}
	if partitionID > 0 {
		if meta, ok := sch.CompressionByPartition[partitionID]; ok {
			return CompressionFromInt(meta.DataCompression)
		}
	}
	if table.DataCompression != schema.CompressionNone {
		return CompressionFromInt(table.DataCompression)
	}
	return CompressionNone
}
