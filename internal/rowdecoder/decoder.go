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
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/uns/mssqllogrecovery/internal/schema"
)

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
// using the provided table schema.
func DecodeRow(data []byte, t *schema.Table) ([]*Value, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("row too short (%d bytes)", len(data))
	}

	// Foffset: end of fixed-length area (absolute from row start, includes the 4-byte header).
	foffset := int(binary.LittleEndian.Uint16(data[2:4]))
	if foffset > len(data) {
		foffset = len(data)
	}

	// Number of columns stored in this row image.
	if foffset+2 > len(data) {
		return nil, fmt.Errorf("row truncated before NumColumns")
	}
	numCols := int(binary.LittleEndian.Uint16(data[foffset : foffset+2]))

	// NULL bitmap.
	nbmapStart := foffset + 2
	nbmapLen := (numCols + 7) / 8
	if nbmapStart+nbmapLen > len(data) {
		return nil, fmt.Errorf("row truncated in NULL bitmap")
	}
	nbmap := data[nbmapStart : nbmapStart+nbmapLen]

	isNull := func(columnID int) bool {
		idx := columnID - 1 // column_id is 1-based
		if idx < 0 || idx >= numCols {
			return true
		}
		return nbmap[idx/8]&(1<<uint(idx%8)) != 0
	}

	// Variable-length section.
	afterNbmap := nbmapStart + nbmapLen
	var varEndOffsets []int
	if afterNbmap+2 <= len(data) {
		numVar := int(binary.LittleEndian.Uint16(data[afterNbmap : afterNbmap+2]))
		offsetArrStart := afterNbmap + 2
		varEndOffsets = make([]int, numVar)
		for i := 0; i < numVar; i++ {
			pos := offsetArrStart + 2*i
			if pos+2 > len(data) {
				break
			}
			// High bit 0x8000 indicates off-row (complex) data; mask it.
			varEndOffsets[i] = int(binary.LittleEndian.Uint16(data[pos:pos+2])) & 0x7FFF
		}
	}
	varDataBase := afterNbmap + 2 + 2*len(varEndOffsets)

	values := make([]*Value, len(t.Columns))
	for i, col := range t.Columns {
		if isNull(col.ColumnID) {
			values[i] = &Value{IsNull: true}
			continue
		}

		if !col.IsFixed {
			vi := col.VarIndex
			if vi >= len(varEndOffsets) {
				values[i] = &Value{IsNull: true}
				continue
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
		return float64(v) / 10000.0, nil

	case schema.TypeMoney:
		// High 4 bytes are the upper int, low 4 bytes are lower int (big-endian halves!).
		hi := int64(int32(binary.LittleEndian.Uint32(b[0:4])))
		lo := int64(binary.LittleEndian.Uint32(b[4:8]))
		v := (hi << 32) | lo
		return float64(v) / 10000.0, nil

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

// decodeDecimal decodes a SQL Server decimal/numeric stored value.
// Layout: sign byte (1=positive) + LE unsigned integer bytes.
func decodeDecimal(b []byte, scale int) float64 {
	if len(b) < 2 {
		return 0
	}
	positive := b[0] == 1
	var mag uint64
	for i := 1; i < len(b) && i <= 8; i++ {
		mag |= uint64(b[i]) << (8 * (i - 1))
	}
	v := float64(mag) / math.Pow10(scale)
	if !positive {
		v = -v
	}
	return v
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
