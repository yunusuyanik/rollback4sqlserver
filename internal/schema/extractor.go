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
    c.is_nullable,
    c.is_identity
FROM sys.columns  c
JOIN sys.objects  o ON c.object_id = o.object_id
JOIN sys.schemas  s ON o.schema_id = s.schema_id
WHERE o.type = 'U'
  AND c.is_computed = 0
ORDER BY o.object_id, c.column_id
`

// physicalColumnsQuery fetches the authoritative physical column set from the
// SQL Server internal partition catalog.  It includes soft-dropped columns
// (absent from sys.columns) that still occupy descriptor nibbles in existing
// compressed rows.  LEFT JOIN with sys.columns supplies name/precision/scale for
// live columns; NULL c.column_id rows are phantom (dropped) columns.
//
// Requires VIEW DATABASE STATE or CONTROL SERVER.  Executed as best-effort:
// failures are silently ignored and compressed decoding falls back to sys.columns.
const physicalColumnsQuery = `
SELECT
    p.object_id,
    p.partition_id,
    pc.partition_column_id,
    pc.system_type_id,
    pc.max_inrow_length,
    CAST(pc.is_nullable AS bit)                     AS is_nullable,
    pc.leaf_offset,
    ISNULL(c.name,      '')                          AS column_name,
    ISNULL(c.max_length, pc.max_inrow_length)        AS max_length,
    ISNULL(c.precision,  0)                          AS precision,
    ISNULL(c.scale,      0)                          AS col_scale,
    ISNULL(c.is_identity, 0)                         AS is_identity,
    CAST(CASE WHEN c.column_id IS NULL THEN 1 ELSE 0 END AS bit) AS is_phantom
FROM sys.partitions p
JOIN sys.system_internals_partition_columns pc
    ON  pc.partition_id = p.partition_id
LEFT JOIN sys.columns c
    ON  c.object_id  = p.object_id
    AND c.column_id  = pc.partition_column_id
    AND c.is_computed = 0
WHERE p.index_id IN (0, 1)
ORDER BY p.object_id, p.partition_id, pc.partition_column_id
`

const pkQuery = `
SELECT ic.object_id, ic.column_id
FROM sys.indexes         i
JOIN sys.index_columns   ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
WHERE i.is_primary_key = 1
ORDER BY ic.object_id, ic.key_ordinal
`

const baseIndexQuery = `
SELECT object_id, name
FROM sys.indexes
WHERE index_id = 1
`

// compressionQuery reads the highest data_compression value for each table's
// base partition (heap = index_id 0, clustered index = index_id 1).
const compressionQuery = `
SELECT p.object_id, MAX(p.data_compression)
FROM sys.partitions p
WHERE p.index_id IN (0, 1)
GROUP BY p.object_id
`

const partitionCompressionQuery = `
SELECT
    p.object_id,
    p.index_id,
    p.partition_number,
    p.partition_id,
    au.allocation_unit_id,
    au.type_desc,
    p.data_compression
FROM sys.partitions p
JOIN sys.allocation_units au
    ON au.container_id = p.hobt_id
    OR au.container_id = p.partition_id
WHERE p.index_id IN (0, 1)
`

// Extract reads table/column metadata from the SQL Server and returns a Schema.
// The connection should be read-only; only SELECT statements are executed.
func Extract(db *sql.DB) (*Schema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sch := &Schema{
		Tables:                 make(map[int]*Table),
		ByName:                 make(map[string]*Table),
		CompressionByAllocUnit: make(map[int64]*PartitionCompression),
		CompressionByPartition: make(map[int64]*PartitionCompression),
		PartitionPhysicalCols:  make(map[int64][]*Column),
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
			isIdentity bool
		)
		if err := rows.Scan(&objectID, &schemaName, &tableName, &columnID, &colName,
			&typeID, &maxLength, &precision, &scale, &isNullable, &isIdentity); err != nil {
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
			IsIdentity: isIdentity,
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

	// Base clustered index name distinguishes real row storage from
	// nonclustered index leaf records that share schema.table in AllocUnitName.
	indexRows, err := db.QueryContext(ctx, baseIndexQuery)
	if err == nil {
		for indexRows.Next() {
			var oid int
			var name string
			if err := indexRows.Scan(&oid, &name); err == nil {
				if t, ok := sch.Tables[oid]; ok {
					t.BaseIndexName = name
				}
			}
		}
		indexRows.Close()
	}

	// Data compression — best-effort; ignore errors (older permissions or SQL Server versions).
	comprRows, err := db.QueryContext(ctx, compressionQuery)
	if err == nil {
		for comprRows.Next() {
			var oid, dc int
			if err := comprRows.Scan(&oid, &dc); err == nil {
				if t, ok := sch.Tables[oid]; ok {
					t.DataCompression = dc
				}
			}
		}
		comprRows.Close()
	}

	partRows, err := db.QueryContext(ctx, partitionCompressionQuery)
	if err == nil {
		for partRows.Next() {
			var (
				objectID, indexID, partNum, dc int
				partitionID                    int64
				allocUnitID                    sql.NullInt64
				allocType                      sql.NullString
			)
			if err := partRows.Scan(&objectID, &indexID, &partNum, &partitionID, &allocUnitID, &allocType, &dc); err != nil {
				continue
			}
			meta := &PartitionCompression{
				ObjectID:        objectID,
				IndexID:         indexID,
				PartitionNumber: partNum,
				PartitionID:     partitionID,
				DataCompression: dc,
			}
			if allocUnitID.Valid {
				meta.AllocUnitID = allocUnitID.Int64
			}
			if allocType.Valid {
				meta.AllocationUnitType = allocType.String
			}
			if t, ok := sch.Tables[objectID]; ok && dc > t.DataCompression {
				t.DataCompression = dc
			}
			sch.CompressionByPartition[partitionID] = meta
			if meta.AllocUnitID > 0 {
				sch.CompressionByAllocUnit[meta.AllocUnitID] = meta
			}
		}
		partRows.Close()
	}

	// Physical columns from sys.system_internals_partition_columns — best-effort.
	// Requires VIEW DATABASE STATE. Failures are silently ignored; compressed decoding
	// falls back to sys.columns order when PhysicalColumns is nil.
	extractPhysicalColumns(ctx, db, sch)

	for _, t := range sch.Tables {
		computeLayout(t)
	}
	return sch, nil
}

// extractPhysicalColumns populates Schema.PartitionPhysicalCols and Table.PhysicalColumns
// from sys.system_internals_partition_columns.  Executed as best-effort; any error is
// silently discarded (caller continues without physical column metadata).
func extractPhysicalColumns(ctx context.Context, db *sql.DB, sch *Schema) {
	rows, err := db.QueryContext(ctx, physicalColumnsQuery)
	if err != nil {
		fmt.Fprintf(os.Stderr, "info: physical columns query skipped (%v); compressed decoding uses sys.columns order\n", err)
		return
	}
	defer rows.Close()

	// Build per-object column_id → Column map from logical schema.
	logicalByObjAndCol := make(map[[2]int]*Column)
	for _, t := range sch.Tables {
		for _, c := range t.Columns {
			logicalByObjAndCol[[2]int{t.ObjectID, c.ColumnID}] = c
		}
	}

	// Collect all rows keyed by (object_id, partition_id).
	type partKey struct {
		objectID    int
		partitionID int64
	}
	partCols := make(map[partKey][]*Column)
	// Track the first partition_id seen for each object (used to set table.PhysicalColumns).
	firstPartition := make(map[int]int64) // object_id → first partition_id

	for rows.Next() {
		var (
			objectID       int
			partitionID    int64
			partColID      int
			systemTypeID   int
			maxInrowLength int
			isNullable     bool
			leafOffset     int
			colName        string
			maxLength      int
			precision      int
			scale          int
			isIdentity     bool
			isPhantom      bool
		)
		if err := rows.Scan(
			&objectID, &partitionID, &partColID,
			&systemTypeID, &maxInrowLength, &isNullable, &leafOffset,
			&colName, &maxLength, &precision, &scale, &isIdentity, &isPhantom,
		); err != nil {
			continue
		}

		var col *Column
		if isPhantom {
			col = &Column{
				ColumnID:   partColID,
				Name:       fmt.Sprintf("_dropped_%d", partColID),
				TypeID:     systemTypeID,
				MaxLength:  maxInrowLength,
				IsNullable: true,
				IsPhantom:  true,
			}
		} else {
			// Prefer logical column definition (richer: precision, scale, etc.).
			if lc, ok := logicalByObjAndCol[[2]int{objectID, partColID}]; ok {
				col = lc
			} else {
				col = &Column{
					ColumnID:   partColID,
					Name:       colName,
					TypeID:     systemTypeID,
					MaxLength:  maxLength,
					Precision:  precision,
					Scale:      scale,
					IsNullable: isNullable,
					IsIdentity: isIdentity,
				}
			}
		}

		k := partKey{objectID, partitionID}
		partCols[k] = append(partCols[k], col)

		if _, seen := firstPartition[objectID]; !seen {
			firstPartition[objectID] = partitionID
		}
	}
	if rows.Err() != nil {
		return
	}

	// Populate PartitionPhysicalCols and Table.PhysicalColumns.
	for k, cols := range partCols {
		sch.PartitionPhysicalCols[k.partitionID] = cols
		if t, ok := sch.Tables[k.objectID]; ok && firstPartition[k.objectID] == k.partitionID {
			t.PhysicalColumns = cols
		}
	}
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
		Tables:                 make(map[int]*Table, len(sf.Tables)),
		ByName:                 make(map[string]*Table, len(sf.Tables)),
		CompressionByAllocUnit: make(map[int64]*PartitionCompression),
		CompressionByPartition: make(map[int64]*PartitionCompression),
		PartitionPhysicalCols:  make(map[int64][]*Column),
	}
	for _, t := range sf.Tables {
		sch.Tables[t.ObjectID] = t
		sch.ByName[t.Schema+"."+t.Name] = t
	}
	return sch, nil
}
