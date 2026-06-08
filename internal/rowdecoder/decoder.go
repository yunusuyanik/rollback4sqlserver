// Package rowdecoder interprets SQL Server row images (RowLog Contents)
// obtained from fn_dump_dblog or fn_dblog into typed Go values.
//
// Row format (non-compressed heap / clustered index, SQL Server 2016+):
//
//	[TagA:1][TagB:1][Foffset:2]   — 4-byte header
//	[fixed-length data]            — bytes 4..Foffset-1
//	[NumColumns:2]                 — at Foffset
//	[NULL bitmap: ceil(n/8) bytes]
//	[NumVarCols:2]
//	[VarColEndOffsets: 2*NumVarCols] — absolute end offsets from row start
//	[variable-length data]
package rowdecoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/uns/mssqllogrecovery/internal/schema"
)

// ErrCompressedRow is returned by DecodeRow when the row image header does not
// match the uncompressed heap/clustered format. This is the expected outcome for
// ROW or PAGE compressed tables — the caller should emit COMPRESSED_ROW_NOT_SUPPORTED
// instead of generating SQL.
var ErrCompressedRow = errors.New("COMPRESSED_ROW_NOT_SUPPORTED")

// ErrSchemaMismatch is returned by DecodeRow when the row's column count or
// column_id layout is inconsistent with the current schema, indicating that the
// schema was changed (DROP COLUMN, or a DROP+ADD combination that produces the
// same column count but different physical layout) after this row was written.
// The caller should emit SCHEMA_MISMATCH instead of generating SQL.
//
// Detection rules (applied before any column is decoded):
//   - numCols (from row header) > len(t.Columns): row has more columns than
//     current schema — a DROP happened after this row was written. The fixed-
//     length area and variable-length offset array have extra entries at
//     positions that computeLayout no longer accounts for.
//   - numCols == len(t.Columns) AND column_ids have gaps: the same column count
//     could be a new row (post-DROP layout) that happens to match an old row
//     (pre-DROP layout) by count alone. With gaps we cannot determine which
//     physical layout the row uses; treating it as ambiguous prevents wrong SQL.
var ErrSchemaMismatch = errors.New("SCHEMA_MISMATCH")

// ErrOffRowLOB is returned by DecodeRow when a non-null variable-length column
// has bit 0x8000 set in its offset array entry, indicating the value is stored
// off-row (LOB pages) and only a pointer descriptor is present in the row image.
// Affected types: varchar(max), nvarchar(max), varbinary(max), text, ntext, image.
// The caller should emit OFF_ROW_LOB_NOT_SUPPORTED instead of generating SQL.
var ErrOffRowLOB = errors.New("OFF_ROW_LOB_NOT_SUPPORTED")

// Value is a decoded column value.
type Value struct {
	IsNull bool
	Raw    interface{} // typed Go value: int64, float64, string, bool, []byte, time.Time, etc.
}

func (v *Value) String() string {
	if v.IsNull {
		return "NULL"
	}
	return fmt.Sprintf("%v", v.Raw)
}

// DecodeRow decodes all columns from a raw row image (Contents0 byte slice)
// using the provided table schema. Returns ErrCompressedRow if the row header
// does not match the uncompressed format (foffset < 4 or > row length).
func DecodeRow(data []byte, t *schema.Table) ([]*Value, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("row too short (%d bytes)", len(data))
	}

	// Foffset: end of fixed-length area (absolute from row start, includes the 4-byte header).
	// For uncompressed rows foffset >= 4 always (the 4-byte header is part of the row).
	// ROW/PAGE compressed rows store a CD array starting at data[0]; data[2:4] is not a
	// valid Foffset and will typically decode to a value < 4 or > len(data).
	foffset := int(binary.LittleEndian.Uint16(data[2:4]))
	if foffset < 4 || foffset > len(data) {
		return nil, fmt.Errorf("%w: foffset=%d len=%d", ErrCompressedRow, foffset, len(data))
	}

	// Number of columns stored in this row image.
	if foffset+2 > len(data) {
		return nil, fmt.Errorf("row truncated before NumColumns")
	}
	numCols := int(binary.LittleEndian.Uint16(data[foffset : foffset+2]))

	// Schema-drift detection: compare the row's column count against the current schema.
	//
	// Case 1 — numCols > len(t.Columns): row was written before a DROP COLUMN. The
	//   fixed-length area and var-offset array have extra slots that computeLayout no
	//   longer accounts for; decoding would read wrong bytes for every column after the
	//   first dropped one.
	//
	// Case 2 — numCols == len(t.Columns) with column_id gaps: a DROP+ADD cycle brought
	//   the count back to the schema count, but the physical layout of old rows differs
	//   from new rows. With gaps we cannot safely distinguish old from new rows by count
	//   alone, so we treat this as ambiguous and refuse to decode.
	hasGaps := false
	for i, col := range t.Columns {
		if col.ColumnID != i+1 {
			hasGaps = true
			break
		}
	}
	if numCols > len(t.Columns) || (numCols == len(t.Columns) && hasGaps) {
		return nil, fmt.Errorf("%w: row numCols=%d schema columns=%d hasGaps=%v",
			ErrSchemaMismatch, numCols, len(t.Columns), hasGaps)
	}

	// NULL bitmap.
	nbmapStart := foffset + 2
	nbmapLen := (numCols + 7) / 8
	if nbmapStart+nbmapLen > len(data) {
		return nil, fmt.Errorf("row truncated in NULL bitmap")
	}
	nbmap := data[nbmapStart : nbmapStart+nbmapLen]

	// isNull uses the ordinal index (loop position i) — not column_id-1 — because
	// the NULL bitmap bit positions are assigned sequentially by the SQL Server engine
	// in the order columns appear on-row. After a DROP COLUMN the surviving columns
	// keep their ordinal positions; using column_id-1 would read the wrong bitmap bit.
	isNull := func(colIdx int) bool {
		if colIdx < 0 || colIdx >= numCols {
			return true
		}
		return nbmap[colIdx/8]&(1<<uint(colIdx%8)) != 0
	}

	// Variable-length section.
	afterNbmap := nbmapStart + nbmapLen
	var varEndOffsets []int
	var offRowMask []bool // offRowMask[i] == true → var-col i has off-row LOB storage
	if afterNbmap+2 <= len(data) {
		numVar := int(binary.LittleEndian.Uint16(data[afterNbmap : afterNbmap+2]))
		offsetArrStart := afterNbmap + 2
		varEndOffsets = make([]int, numVar)
		offRowMask = make([]bool, numVar)
		for i := 0; i < numVar; i++ {
			pos := offsetArrStart + 2*i
			if pos+2 > len(data) {
				break
			}
			raw := binary.LittleEndian.Uint16(data[pos : pos+2])
			// Bit 15 (0x8000): off-row/complex storage. The bytes at the offset position
			// are a LOB pointer (16 bytes for text/ntext/image, 24 bytes for *max types),
			// not the actual column value. Record which var-cols are off-row; the column
			// loop below will return ErrOffRowLOB rather than decoding the pointer as data.
			offRowMask[i] = raw&0x8000 != 0
			varEndOffsets[i] = int(raw & 0x7FFF)
		}
	}
	varDataBase := afterNbmap + 2 + 2*len(varEndOffsets)

	values := make([]*Value, len(t.Columns))
	for i, col := range t.Columns {
		if isNull(i) {
			values[i] = &Value{IsNull: true}
			continue
		}

		if !col.IsFixed {
			vi := col.VarIndex
			if vi >= len(varEndOffsets) {
				values[i] = &Value{IsNull: true}
				continue
			}
			// Off-row flag set → only a LOB pointer is stored in the row, not the value.
			// Decoding the pointer as column data would produce garbage SQL.
			if vi < len(offRowMask) && offRowMask[vi] {
				return nil, fmt.Errorf("%w: column %q (var index %d)", ErrOffRowLOB, col.Name, vi)
			}
			endOff := varEndOffsets[vi]
			startOff := varDataBase
			if vi > 0 {
				startOff = varEndOffsets[vi-1]
			}
			if startOff > len(data) || endOff > len(data) || startOff > endOff {
				values[i] = &Value{IsNull: true}
				continue
			}
			raw, err := decodeVar(col, data[startOff:endOff])
			if err != nil {
				values[i] = &Value{IsNull: true}
				continue
			}
			values[i] = &Value{Raw: raw}
			continue
		}

		// Fixed-length column (non-bit).
		if col.TypeID == schema.TypeBit {
			bytePos := 4 + col.BitByteOffset
			if bytePos >= len(data) {
				values[i] = &Value{IsNull: true}
				continue
			}
			b := (data[bytePos] >> uint(col.BitOffset)) & 1
			values[i] = &Value{Raw: b == 1}
			continue
		}

		start := 4 + col.FixedOffset
		size := schema.FixedSize(col)
		end := start + size
		if end > len(data) || end > foffset {
			values[i] = &Value{IsNull: true}
			continue
		}

		raw, err := decodeFixed(col, data[start:end])
		if err != nil {
			values[i] = &Value{IsNull: true}
			continue
		}
		values[i] = &Value{Raw: raw}
	}
	return values, nil
}

// ModifyRowDelta holds decoded column values extracted from a LOP_MODIFY_ROW
// delta record. Indexed by t.Columns position; nil = column not in the delta.
type ModifyRowDelta struct {
	RowOffset int      // byte offset within the row where the modification starts
	Before    []*Value // before-values of columns in the changed region
	After     []*Value // after-values of columns in the changed region
}

// ParseModifyRowDelta extracts before/after values for fixed-length columns
// from a LOP_MODIFY_ROW log record binary (the [Log Record] column in fn_dblog).
//
// SQL Server 2016+ layout at the end of [Log Record]:
//
//	[...header...][RowOffset:2][OldLen:2][NewLen:2][OldData:OldLen][NewData:NewLen]
//
// OldData == Contents1, NewData == Contents0.  We fingerprint by verifying that
// Contents0 and Contents1 appear verbatim at the end of the binary, then sanity-
// check that the embedded lengths match.  Variable-length columns are skipped
// (their positions shift with the delta; only fixed-length layout is stable).
//
// Returns nil when the binary format is not recognised — callers should fall
// back to the existing allNull heuristic.
func ParseModifyRowDelta(logRecord, c0, c1 []byte, t *schema.Table) *ModifyRowDelta {
	rowOff, ok := findModifyRowOffset(logRecord, c0, c1)
	if !ok {
		return nil
	}
	c0l, c1l := len(c0), len(c1)

	before := make([]*Value, len(t.Columns))
	after := make([]*Value, len(t.Columns))

	for i, col := range t.Columns {
		if !col.IsFixed {
			continue
		}

		if col.TypeID == schema.TypeBit {
			bytePos := 4 + col.BitByteOffset
			if bytePos >= rowOff && bytePos+1 <= rowOff+c1l && bytePos-rowOff < len(c1) {
				b := (c1[bytePos-rowOff] >> uint(col.BitOffset)) & 1
				before[i] = &Value{Raw: b == 1}
			}
			if bytePos >= rowOff && bytePos+1 <= rowOff+c0l && bytePos-rowOff < len(c0) {
				b := (c0[bytePos-rowOff] >> uint(col.BitOffset)) & 1
				after[i] = &Value{Raw: b == 1}
			}
			continue
		}

		colStart := 4 + col.FixedOffset
		size := schema.FixedSize(col)
		colEnd := colStart + size

		if colStart >= rowOff && colEnd <= rowOff+c1l {
			rel := colStart - rowOff
			if rel+size <= len(c1) {
				if raw, err := decodeFixed(col, c1[rel:rel+size]); err == nil {
					before[i] = &Value{Raw: raw}
				}
			}
		}
		if colStart >= rowOff && colEnd <= rowOff+c0l {
			rel := colStart - rowOff
			if rel+size <= len(c0) {
				if raw, err := decodeFixed(col, c0[rel:rel+size]); err == nil {
					after[i] = &Value{Raw: raw}
				}
			}
		}
	}
	return &ModifyRowDelta{RowOffset: rowOff, Before: before, After: after}
}

// findModifyRowOffset locates the row modification offset inside the raw [Log Record]
// binary. It verifies that Contents0 (new bytes) sits at the very end and Contents1
// (old bytes) sits immediately before it, then reads the 2-byte offset field that
// precedes the length fields.
func findModifyRowOffset(logRecord, c0, c1 []byte) (offset int, ok bool) {
	n := len(logRecord)
	c0l, c1l := len(c0), len(c1)
	if c0l == 0 || c1l == 0 || n < c0l+c1l+6 {
		return 0, false
	}
	if !bytes.Equal(logRecord[n-c0l:], c0) {
		return 0, false
	}
	if !bytes.Equal(logRecord[n-c0l-c1l:n-c0l], c1) {
		return 0, false
	}
	base := n - c0l - c1l
	if base < 6 {
		return 0, false
	}
	// Layout before [c1][c0]: [RowOffset:2][OldLen:2][NewLen:2]
	oldLen := int(binary.LittleEndian.Uint16(logRecord[base-4 : base-2]))
	newLen := int(binary.LittleEndian.Uint16(logRecord[base-2 : base]))
	if oldLen != c1l || newLen != c0l {
		return 0, false // length mismatch — layout assumption wrong
	}
	return int(binary.LittleEndian.Uint16(logRecord[base-6 : base-4])), true
}

// ReconstructBeforeImage attempts to reconstruct the before-image for
// LOP_MODIFY_ROW. contents0 = after image, contents1 = XOR delta.
// The XOR is applied over the entire row (safe since unchanged bytes XOR to 0).
func ReconstructBeforeImage(contents0, contents1 []byte) []byte {
	if len(contents1) == 0 {
		return contents0
	}
	before := make([]byte, len(contents0))
	copy(before, contents0)
	for i := 0; i < len(contents1) && i < len(before); i++ {
		before[i] ^= contents1[i]
	}
	return before
}

// --- Fixed-length decoders ---

func decodeFixed(col *schema.Column, b []byte) (interface{}, error) {
	switch col.TypeID {
	case schema.TypeTinyint:
		return int64(b[0]), nil

	case schema.TypeSmallint:
		return int64(int16(binary.LittleEndian.Uint16(b))), nil

	case schema.TypeInt, schema.TypeSmalldatetime:
		if col.TypeID == schema.TypeSmalldatetime {
			days := int(binary.LittleEndian.Uint16(b[0:2]))
			mins := int(binary.LittleEndian.Uint16(b[2:4]))
			base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
			t := base.AddDate(0, 0, days).Add(time.Duration(mins) * time.Minute)
			return t, nil
		}
		return int64(int32(binary.LittleEndian.Uint32(b))), nil

	case schema.TypeBigint:
		return int64(binary.LittleEndian.Uint64(b)), nil

	case schema.TypeReal:
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil

	case schema.TypeFloat:
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil

	case schema.TypeSmallmoney:
		v := int32(binary.LittleEndian.Uint32(b))
		return formatMoneyScaled(int64(v)), nil

	case schema.TypeMoney:
		// SQL Server stores MONEY as two LE int32 halves: [high][low].
		hi := int64(int32(binary.LittleEndian.Uint32(b[0:4])))
		lo := int64(binary.LittleEndian.Uint32(b[4:8]))
		return formatMoneyScaled((hi << 32) | lo), nil

	case schema.TypeDatetime:
		days := int32(binary.LittleEndian.Uint32(b[0:4]))
		ticks := binary.LittleEndian.Uint32(b[4:8]) // 1/300 seconds since midnight
		base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		t := base.AddDate(0, 0, int(days))
		ns := int64(ticks) * int64(time.Second) * 10 / 3 // 1/300 s = 10/3 ms ≈ 3333333 ns
		t = t.Add(time.Duration(ns))
		return t, nil

	case schema.TypeDate:
		days := int(b[0]) | int(b[1])<<8 | int(b[2])<<16
		base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
		return base.AddDate(0, 0, days), nil

	case schema.TypeTime:
		return decodeTime(b, col.Scale), nil

	case schema.TypeDatetime2:
		timeBytes := schema.FixedSize(&schema.Column{TypeID: schema.TypeTime, Scale: col.Scale})
		t := decodeTime(b[:timeBytes], col.Scale)
		dateBytes := b[timeBytes:]
		days := int(dateBytes[0]) | int(dateBytes[1])<<8 | int(dateBytes[2])<<16
		base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)
		return time.Date(base.Year(), base.Month(), base.Day(),
			t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), nil

	case schema.TypeDatetimeoffset:
		timeBytes := schema.FixedSize(&schema.Column{TypeID: schema.TypeTime, Scale: col.Scale})
		t := decodeTime(b[:timeBytes], col.Scale)
		dateBytes := b[timeBytes : timeBytes+3]
		days := int(dateBytes[0]) | int(dateBytes[1])<<8 | int(dateBytes[2])<<16
		base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)
		offsetMins := int(int16(binary.LittleEndian.Uint16(b[timeBytes+3:])))
		loc := time.FixedZone("", offsetMins*60)
		return time.Date(base.Year(), base.Month(), base.Day(),
			t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc), nil

	case schema.TypeUniqueidentifier:
		// SQL Server stores GUID with mixed-endian layout:
		// first 4 bytes LE, next 2 bytes LE, next 2 bytes LE, last 8 bytes BE.
		return fmt.Sprintf("%08X-%04X-%04X-%04X-%X",
			binary.LittleEndian.Uint32(b[0:4]),
			binary.LittleEndian.Uint16(b[4:6]),
			binary.LittleEndian.Uint16(b[6:8]),
			binary.BigEndian.Uint16(b[8:10]),
			b[10:16]), nil

	case schema.TypeChar, schema.TypeBinary:
		if col.TypeID == schema.TypeBinary {
			out := make([]byte, len(b))
			copy(out, b)
			return out, nil
		}
		return strings.TrimRight(string(b), " "), nil

	case schema.TypeNchar:
		u16 := make([]uint16, len(b)/2)
		for i := range u16 {
			u16[i] = binary.LittleEndian.Uint16(b[2*i : 2*i+2])
		}
		s := string(utf16.Decode(u16))
		return strings.TrimRight(s, " "), nil

	case schema.TypeNumeric, schema.TypeDecimal:
		return decodeDecimal(b, col.Scale), nil
	}

	// Fallback: return raw bytes.
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// decodeTime decodes a SQL Server time(n) value (3-5 bytes LE scaled integer).
func decodeTime(b []byte, scale int) time.Time {
	var ticks int64
	for i, byt := range b {
		ticks |= int64(byt) << (8 * i)
	}
	// ticks = time in units of 10^-scale seconds
	ns := ticks * int64(math.Pow10(9-scale))
	h := ns / int64(time.Hour)
	ns -= h * int64(time.Hour)
	m := ns / int64(time.Minute)
	ns -= m * int64(time.Minute)
	s := ns / int64(time.Second)
	ns -= s * int64(time.Second)
	return time.Date(0, 1, 1, int(h), int(m), int(s), int(ns), time.UTC)
}

// formatMoneyScaled formats a SQL Server MONEY or SMALLMONEY scaled integer
// (4 implied decimal places) as an exact decimal string with no float64.
// Handles the full MONEY range including MinInt64 via uint64 arithmetic.
func formatMoneyScaled(scaled int64) string {
	negative := scaled < 0
	// uint64 conversion of a negative int64 via two's complement gives the
	// correct absolute value even for MinInt64 (Go spec §Conversions).
	var mag uint64
	if negative {
		mag = uint64(-scaled)
	} else {
		mag = uint64(scaled)
	}
	intPart := mag / 10000
	fracPart := mag % 10000
	result := fmt.Sprintf("%d.%04d", intPart, fracPart)
	if negative {
		result = "-" + result
	}
	return result
}

// decodeDecimal decodes a SQL Server decimal/numeric stored value into an
// exact decimal string. No float64 is used; precision up to DECIMAL(38) is
// fully supported.
//
// Layout: b[0] = sign (1=positive, 0=negative), b[1:] = little-endian
// unsigned integer magnitude. Total length from decimalSize(): 5/9/13/17.
func decodeDecimal(b []byte, scale int) string {
	if len(b) < 2 {
		return "0"
	}
	positive := b[0] == 1

	// Reverse LE bytes to BE for big.Int.SetBytes.
	valueBytes := b[1:]
	rev := make([]byte, len(valueBytes))
	for i, byt := range valueBytes {
		rev[len(valueBytes)-1-i] = byt
	}
	mag := new(big.Int).SetBytes(rev)

	s := mag.String() // unscaled decimal digits

	var result string
	if scale == 0 {
		result = s
	} else {
		// Pad with leading zeros so there are at least scale+1 digits.
		for len(s) <= scale {
			s = "0" + s
		}
		result = s[:len(s)-scale] + "." + s[len(s)-scale:]
	}

	if !positive && mag.Sign() != 0 {
		result = "-" + result
	}
	return result
}

// --- Variable-length decoders ---

func decodeVar(col *schema.Column, b []byte) (interface{}, error) {
	switch col.TypeID {
	case schema.TypeVarchar, schema.TypeText:
		return string(b), nil

	case schema.TypeNvarchar, schema.TypeNtext:
		if len(b)%2 != 0 {
			b = b[:len(b)-1]
		}
		u16 := make([]uint16, len(b)/2)
		for i := range u16 {
			u16[i] = binary.LittleEndian.Uint16(b[2*i : 2*i+2])
		}
		return string(utf16.Decode(u16)), nil

	case schema.TypeVarbinary, schema.TypeImage:
		out := make([]byte, len(b))
		copy(out, b)
		return out, nil

	case schema.TypeXML:
		return string(b), nil
	}

	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}
