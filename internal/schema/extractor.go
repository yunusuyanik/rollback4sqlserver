package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

const schemaQuery = `
SELECT
    o.object_id,
    s.name  AS schema_name,
    o.name  AS table_name,
    c.column_id,
    c.name  AS column_name,
    c.system_type_id,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable
FROM sys.columns  c
JOIN sys.objects  o ON c.object_id = o.object_id
JOIN sys.schemas  s ON o.schema_id = s.schema_id
WHERE o.type = 'U'
  AND c.is_computed = 0
ORDER BY o.object_id, c.column_id
`

const pkQuery = `
SELECT ic.object_id, ic.column_id
FROM sys.indexes         i
JOIN sys.index_columns   ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE i.is_primary_key = 1
ORDER BY ic.object_id, ic.key_ordinal
`

// Extract reads table/column metadata from the SQL Server and returns a Schema.
// The connection should be read-only; only SELECT statements are executed.
func Extract(db *sql.DB) (*Schema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sch := &Schema{
		Tables: make(map[int]*Table),
		ByName: make(map[string]*Table),
	}

	rows, err := db.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("schema query: %w", err)
	}

	for rows.Next() {
		var (
			objectID   int
			schemaName string
			tableName  string
			columnID   int
			colName    string
			typeID     int
			maxLength  int
			precision  int
			scale      int
			isNullable bool
		)
		if err := rows.Scan(&objectID, &schemaName, &tableName, &columnID, &colName,
			&typeID, &maxLength, &precision, &scale, &isNullable); err != nil {
			rows.Close()
			return nil, fmt.Errorf("schema scan: %w", err)
		}

		t, ok := sch.Tables[objectID]
		if !ok {
			t = &Table{ObjectID: objectID, Schema: schemaName, Name: tableName}
			sch.Tables[objectID] = t
			sch.ByName[schemaName+"."+tableName] = t
		}

		t.Columns = append(t.Columns, &Column{
			ColumnID:   columnID,
			Name:       colName,
			TypeID:     typeID,
			MaxLength:  maxLength,
			Precision:  precision,
			Scale:      scale,
			IsNullable: isNullable,
		})
	}
	rowsErr := rows.Err()
	rows.Close() // explicit close — frees the connection back to pool before next query
	if rowsErr != nil {
		return nil, rowsErr
	}

	// PK columns — reuses the same physical connection now that rows is closed.
	pkRows, err := db.QueryContext(ctx, pkQuery)
	if err == nil {
		for pkRows.Next() {
			var oid, cid int
			if err := pkRows.Scan(&oid, &cid); err == nil {
				if t, ok := sch.Tables[oid]; ok {
					t.PKCols = append(t.PKCols, cid)
				}
			}
		}
		pkRows.Close()
	}

	for _, t := range sch.Tables {
		computeLayout(t)
	}
	return sch, nil
}

// computeLayout fills in IsFixed, FixedOffset, VarIndex, BitByteOffset, BitOffset for each column.
func computeLayout(t *Table) {
	fixedOff := 0
	varIdx := 0
	bitByteOff := -1
	bitBitOff := 8 // ≥8 forces a new byte on first bit column

	for _, col := range t.Columns {
		if isVarLen(col.TypeID) || col.MaxLength < 0 {
			col.IsFixed = false
			col.VarIndex = varIdx
			varIdx++
			continue
		}

		col.IsFixed = true

		if col.TypeID == TypeBit {
			if bitBitOff >= 8 {
				bitByteOff = fixedOff
				fixedOff++
				bitBitOff = 0
			}
			col.BitByteOffset = bitByteOff
			col.BitOffset = bitBitOff
			bitBitOff++
			continue
		}

		// Non-bit fixed column breaks an in-progress bit byte.
		bitBitOff = 8

		col.FixedOffset = fixedOff
		fixedOff += FixedSize(col)
	}
}

func isVarLen(typeID int) bool {
	switch typeID {
	case TypeVarchar, TypeNvarchar, TypeVarbinary, TypeText, TypeNtext, TypeImage, TypeXML, TypeSQLVariant:
		return true
	}
	return false
}

// FixedSize returns the byte size of a fixed-length column on the row.
// Must not be called for TypeBit or variable-length types.
func FixedSize(col *Column) int {
	switch col.TypeID {
	case TypeTinyint:
		return 1
	case TypeSmallint:
		return 2
	case TypeInt, TypeReal, TypeSmallmoney, TypeSmalldatetime:
		return 4
	case TypeBigint, TypeMoney, TypeFloat, TypeDatetime:
		return 8
	case TypeDate:
		return 3
	case TypeTime:
		return timeSize(col.Scale)
	case TypeDatetime2:
		return datetime2Size(col.Scale)
	case TypeDatetimeoffset:
		return datetimeoffsetSize(col.Scale)
	case TypeUniqueidentifier:
		return 16
	case TypeChar, TypeBinary:
		return col.MaxLength
	case TypeNchar:
		return col.MaxLength // stored as UTF-16LE, max_length already in bytes
	case TypeNumeric, TypeDecimal:
		return decimalSize(col.Precision)
	}
	return col.MaxLength
}

func timeSize(scale int) int {
	switch {
	case scale <= 2:
		return 3
	case scale <= 4:
		return 4
	default:
		return 5
	}
}

func datetime2Size(scale int) int {
	switch {
	case scale <= 2:
		return 6
	case scale <= 4:
		return 7
	default:
		return 8
	}
}

func datetimeoffsetSize(scale int) int {
	switch {
	case scale <= 2:
		return 8
	case scale <= 4:
		return 9
	default:
		return 10
	}
}

func decimalSize(precision int) int {
	switch {
	case precision <= 9:
		return 5
	case precision <= 19:
		return 9
	case precision <= 28:
		return 13
	default:
		return 17
	}
}

// schemaFile is the on-disk JSON representation.
type schemaFile struct {
	Tables []*Table `json:"tables"`
}

// Save writes the schema to a JSON file.
func Save(sch *Schema, path string) error {
	tables := make([]*Table, 0, len(sch.Tables))
	for _, t := range sch.Tables {
		tables = append(tables, t)
	}
	data, err := json.MarshalIndent(schemaFile{Tables: tables}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// Load reads a previously saved schema JSON file.
func Load(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var sf schemaFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, err
	}
	sch := &Schema{
		Tables: make(map[int]*Table, len(sf.Tables)),
		ByName: make(map[string]*Table, len(sf.Tables)),
	}
	for _, t := range sf.Tables {
		sch.Tables[t.ObjectID] = t
		sch.ByName[t.Schema+"."+t.Name] = t
	}
	return sch, nil
}
