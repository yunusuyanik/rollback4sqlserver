package schema

import (
	"fmt"
	"strings"
)

// DataCompression values — match sys.partitions.data_compression.
const (
	CompressionNone = 0
	CompressionRow  = 1
	CompressionPage = 2
)

// SQL Server system_type_id constants from sys.types
const (
	TypeBit              = 104
	TypeTinyint          = 48
	TypeSmallint         = 52
	TypeInt              = 56
	TypeBigint           = 127
	TypeReal             = 59
	TypeFloat            = 62
	TypeSmallmoney       = 122
	TypeMoney            = 60
	TypeSmalldatetime    = 58
	TypeDatetime         = 61
	TypeDate             = 40
	TypeTime             = 41
	TypeDatetime2        = 42
	TypeDatetimeoffset   = 43
	TypeUniqueidentifier = 36
	TypeChar             = 175
	TypeNchar            = 239
	TypeBinary           = 173
	TypeNumeric          = 108
	TypeDecimal          = 106
	TypeVarchar          = 167
	TypeNvarchar         = 231
	TypeVarbinary        = 165
	TypeText             = 35
	TypeNtext            = 99
	TypeImage            = 34
	TypeXML              = 241
	TypeSQLVariant       = 98
)

// Column describes one column in a table, including its computed physical layout.
type Column struct {
	ColumnID   int    `json:"column_id"`
	Name       string `json:"name"`
	TypeID     int    `json:"type_id"`
	MaxLength  int    `json:"max_length"` // bytes; -1 = MAX
	Precision  int    `json:"precision"`
	Scale      int    `json:"scale"`
	IsNullable bool   `json:"is_nullable"`
	IsIdentity bool   `json:"is_identity,omitempty"`

	// IsPhantom marks columns that exist in sys.system_internals_partition_columns
	// (physical storage) but not in sys.columns — i.e. soft-dropped columns that
	// still occupy a descriptor nibble in existing compressed rows.
	IsPhantom bool `json:"is_phantom,omitempty"`

	// Computed physical layout (populated by computeLayout)
	IsFixed       bool `json:"is_fixed"`
	FixedOffset   int  `json:"fixed_offset"`    // byte offset within fixed-length area (after the 4-byte row header)
	VarIndex      int  `json:"var_index"`       // 0-based index within variable-length columns
	BitByteOffset int  `json:"bit_byte_offset"` // byte offset of bit-packing byte (TypeBit only)
	BitOffset     int  `json:"bit_offset"`      // bit position within bit byte 0-7 (TypeBit only)
}

// PartitionCompression describes compression for one partition/allocation unit.
type PartitionCompression struct {
	ObjectID           int    `json:"object_id"`
	IndexID            int    `json:"index_id"`
	PartitionNumber    int    `json:"partition_number"`
	PartitionID        int64  `json:"partition_id"`
	AllocUnitID        int64  `json:"allocation_unit_id"`
	AllocationUnitType string `json:"allocation_unit_type"`
	DataCompression    int    `json:"data_compression"`
}

// Table describes a user table and its ordered columns.
type Table struct {
	ObjectID      int       `json:"object_id"`
	Schema        string    `json:"schema"`
	Name          string    `json:"name"`
	BaseIndexName string    `json:"base_index_name,omitempty"`
	PKCols        []int     `json:"pk_cols"` // column_ids of PK columns in key order
	Columns       []*Column `json:"columns"`

	// DataCompression is the highest data_compression among base partitions
	// (index_id 0 or 1). Used as fallback when AllocUnitID/PartitionID lookup misses.
	DataCompression int `json:"data_compression,omitempty"`

	// PhysicalColumns is the authoritative column set from sys.system_internals_partition_columns,
	// ordered by partition_column_id. It may include soft-dropped or internal columns (IsPhantom=true)
	// that still occupy descriptor nibbles in compressed rows but are absent from Columns.
	// Non-nil only when the physical column query succeeded. Used by the compressed row decoder.
	PhysicalColumns []*Column `json:"physical_columns,omitempty"`
}

// Schema holds schema info for all user tables, indexed for fast lookup.
type Schema struct {
	Tables map[int]*Table    `json:"tables"` // object_id → Table
	ByName map[string]*Table `json:"-"`      // "schema.table" → Table (rebuilt on load)

	// CompressionByAllocUnit maps allocation_unit_id → partition compression metadata.
	CompressionByAllocUnit map[int64]*PartitionCompression `json:"-"`
	// CompressionByPartition maps partition_id → partition compression metadata.
	CompressionByPartition map[int64]*PartitionCompression `json:"-"`

	// PartitionPhysicalCols maps partition_id → physical column list (in partition_column_id order).
	// More precise than Table.PhysicalColumns for tables with many partitions.
	PartitionPhysicalCols map[int64][]*Column `json:"-"`
}

// Lookup finds a table by its AllocUnitName (e.g. "dbo.Orders" or "dbo.Orders.PK_Orders").
func (s *Schema) Lookup(allocUnitName string) *Table {
	// AllocUnitName can be "schema.table" or "schema.table.index"
	// Use ByName which is keyed on "schema.table"
	parts := splitDots(allocUnitName, 3)
	if len(parts) < 2 {
		return nil
	}
	key := parts[0] + "." + parts[1]
	return s.ByName[key]
}

// LookupStorage resolves a base table from the log record's storage metadata.
// fn_dump_dblog often returns "Unknown Alloc Unit" for archived records even
// though AllocUnitId and PartitionId are populated.
func (s *Schema) LookupStorage(allocUnitName string, allocUnitID, partitionID int64) *Table {
	if allocUnitID > 0 {
		if meta, ok := s.CompressionByAllocUnit[allocUnitID]; ok {
			return s.Tables[meta.ObjectID]
		}
	}
	if partitionID > 0 {
		if meta, ok := s.CompressionByPartition[partitionID]; ok {
			return s.Tables[meta.ObjectID]
		}
	}
	parts := splitDots(allocUnitName, 3)
	if len(parts) < 2 {
		return nil
	}
	table := s.ByName[parts[0]+"."+parts[1]]
	if table == nil || len(parts) == 2 {
		return table
	}
	if table.BaseIndexName != "" && strings.EqualFold(parts[2], table.BaseIndexName) {
		return table
	}
	return nil
}

// IsKnownNonBaseIndex reports whether AllocUnitName names a known table's
// nonclustered index. Such leaf records are index entries, not base row images.
func (s *Schema) IsKnownNonBaseIndex(allocUnitName string) bool {
	parts := splitDots(allocUnitName, 3)
	if len(parts) != 3 {
		return false
	}
	table := s.ByName[parts[0]+"."+parts[1]]
	if table == nil {
		return false
	}
	return table.BaseIndexName == "" || !strings.EqualFold(parts[2], table.BaseIndexName)
}

// TypeName returns the lowercase SQL Server type name for a system_type_id.
func TypeName(typeID int) string {
	switch typeID {
	case TypeBit:
		return "bit"
	case TypeTinyint:
		return "tinyint"
	case TypeSmallint:
		return "smallint"
	case TypeInt:
		return "int"
	case TypeBigint:
		return "bigint"
	case TypeReal:
		return "real"
	case TypeFloat:
		return "float"
	case TypeSmallmoney:
		return "smallmoney"
	case TypeMoney:
		return "money"
	case TypeSmalldatetime:
		return "smalldatetime"
	case TypeDatetime:
		return "datetime"
	case TypeDate:
		return "date"
	case TypeTime:
		return "time"
	case TypeDatetime2:
		return "datetime2"
	case TypeDatetimeoffset:
		return "datetimeoffset"
	case TypeUniqueidentifier:
		return "uniqueidentifier"
	case TypeChar:
		return "char"
	case TypeNchar:
		return "nchar"
	case TypeBinary:
		return "binary"
	case TypeNumeric:
		return "numeric"
	case TypeDecimal:
		return "decimal"
	case TypeVarchar:
		return "varchar"
	case TypeNvarchar:
		return "nvarchar"
	case TypeVarbinary:
		return "varbinary"
	case TypeText:
		return "text"
	case TypeNtext:
		return "ntext"
	case TypeImage:
		return "image"
	case TypeXML:
		return "xml"
	case TypeSQLVariant:
		return "sql_variant"
	default:
		return fmt.Sprintf("type_%d", typeID)
	}
}

// splitDots splits s on '.' up to maxParts parts.
func splitDots(s string, maxParts int) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s) && len(parts) < maxParts-1; i++ {
		if s[i] == '.' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
