package schema

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

	// Computed physical layout (populated by computeLayout)
	IsFixed       bool `json:"is_fixed"`
	FixedOffset   int  `json:"fixed_offset"`    // byte offset within fixed-length area (after the 4-byte row header)
	VarIndex      int  `json:"var_index"`        // 0-based index within variable-length columns
	BitByteOffset int  `json:"bit_byte_offset"`  // byte offset of bit-packing byte (TypeBit only)
	BitOffset     int  `json:"bit_offset"`        // bit position within bit byte 0-7 (TypeBit only)
}

// Table describes a user table and its ordered columns.
type Table struct {
	ObjectID int       `json:"object_id"`
	Schema   string    `json:"schema"`
	Name     string    `json:"name"`
	PKCols   []int     `json:"pk_cols"` // column_ids of PK columns in key order
	Columns  []*Column `json:"columns"`
}

// Schema holds schema info for all user tables, indexed for fast lookup.
type Schema struct {
	Tables map[int]*Table    `json:"tables"` // object_id → Table
	ByName map[string]*Table `json:"-"`      // "schema.table" → Table (rebuilt on load)
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
