package mssql

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/uns/mssqllogrecovery/internal/rowdecoder"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

// MSSQLCompressedRowDecoder decodes SQL Server ROW/PAGE compressed row images.
type MSSQLCompressedRowDecoder struct{}

func (d *MSSQLCompressedRowDecoder) Decode(row []byte, table *schema.Table, compression CompressionType) (DecodedRow, error) {
	if len(row) < 2 {
		return DecodedRow{}, fmt.Errorf("%w: row too short (%d bytes)", ErrCompressedRowParseFailed, len(row))
	}
	if tag := row[0] & 0x30; tag != 0x20 && tag != 0x30 {
		return DecodedRow{}, fmt.Errorf("%w: invalid compressed row header 0x%02X", ErrCompressedRowParseFailed, row[0])
	}

	// Use physical columns from sys.system_internals_partition_columns when available.
	// Physical columns may include phantom (soft-dropped) columns that still occupy
	// descriptor nibbles in existing compressed rows but are absent from table.Columns.
	// Without this, the nibble count is wrong → payloadStart off → all reads cascade into errors.
	physCols := table.Columns
	usingPhysical := false
	if len(table.PhysicalColumns) > 0 {
		physCols = table.PhysicalColumns
		usingPhysical = true
	}

	slices, payloadStart, tailCols, err := ParseCompressedRowLayout(row, physCols)
	if err != nil {
		return DecodedRow{}, err
	}

	debug := CompressionDebugInfo{
		CompressionType:      string(compression),
		PayloadStart:         payloadStart,
		TailColumns:          tailCols,
		PhysicalColumnCount:  len(physCols),
		LogicalColumnCount:   len(table.Columns),
		UsingPhysicalColumns: usingPhysical,
	}
	for _, sl := range slices {
		debug.DescriptorNibbles = append(debug.DescriptorNibbles, fmt.Sprintf("%X", sl.Descriptor))
	}
	for _, sl := range slices {
		if sl.IsPageReference {
			debug.Warning = "row contains PAGE compression prefix/dictionary references; the row log image alone is insufficient"
			return DecodedRow{CompressionDebug: debug}, fmt.Errorf(
				"%w: column %q descriptor=0x%X",
				ErrCompressedPageReference, sl.Column.Name, sl.Descriptor,
			)
		}
	}

	// Decode each physical column's value; phantom columns are skipped (no logical mapping).
	// physValues maps column_id → decoded value for non-phantom columns.
	physValues := make(map[int]*rowdecoder.Value, len(physCols))
	var partialErrors int
	for _, sl := range slices {
		colDebug := CompressionColumnDebug{
			Name:           sl.Column.Name,
			Type:           schema.TypeName(sl.Column.TypeID),
			Descriptor:     fmt.Sprintf("%X", sl.Descriptor),
			Offset:         sl.Offset,
			PhysicalLength: sl.PhysicalLength,
			LogicalLength:  sl.LogicalLength,
			CompressedHex:  hex.EncodeToString(sl.CompressedValue),
		}

		if sl.Column.IsPhantom {
			// Dropped column: its descriptor nibble and bytes were consumed by ParseCompressedRowLayout;
			// no value to decode or map.
			colDebug.Error = "phantom (dropped) column — skipped"
			debug.Columns = append(debug.Columns, colDebug)
			continue
		}

		colID := sl.Column.ColumnID

		if sl.Column.TypeID == schema.TypeBit {
			val, err := decodeCompressedBit(sl)
			if err != nil {
				colDebug.Error = err.Error()
				debug.Columns = append(debug.Columns, colDebug)
				physValues[colID] = &rowdecoder.Value{IsNull: true}
				partialErrors++
				continue
			}
			physValues[colID] = val
			debug.Columns = append(debug.Columns, colDebug)
			continue
		}

		if sl.IsNull {
			physValues[colID] = &rowdecoder.Value{IsNull: true}
			debug.Columns = append(debug.Columns, colDebug)
			continue
		}

		raw, unhex, err := decodeCompressedColumnValue(sl)
		if err != nil {
			colDebug.Error = err.Error()
			debug.Columns = append(debug.Columns, colDebug)
			physValues[colID] = &rowdecoder.Value{IsNull: true}
			partialErrors++
			continue
		}
		colDebug.UncompressedHex = hex.EncodeToString(unhex)
		physValues[colID] = &rowdecoder.Value{Raw: raw}
		debug.Columns = append(debug.Columns, colDebug)
	}

	// Map physical values to logical column order by column_id.
	values := make([]*rowdecoder.Value, len(table.Columns))
	for i, col := range table.Columns {
		if v, ok := physValues[col.ColumnID]; ok {
			values[i] = v
		} else {
			values[i] = &rowdecoder.Value{IsNull: true}
		}
	}

	if partialErrors > 0 {
		debug.Warning = fmt.Sprintf("%d column(s) set to NULL due to decode errors", partialErrors)
	}
	return DecodedRow{Values: values, CompressionDebug: debug}, nil
}

// ExtractCompressedColumnNibbles reads per-column descriptor nibbles from a compressed row.
// The first two bytes are the compressed record status/header and are not descriptors.
func ExtractCompressedColumnNibbles(row []byte, columnCount int) ([]byte, int, error) {
	if len(row) < 2 {
		return nil, 0, fmt.Errorf("row shorter than compressed header")
	}
	nibbleCount := columnCount
	if nibbleCount%2 != 0 {
		nibbleCount++
	}
	descriptorBytes := nibbleCount / 2
	payloadStart := 2 + descriptorBytes
	if len(row) < payloadStart {
		return nil, 0, fmt.Errorf("row truncated in descriptor area")
	}

	nibbles := make([]byte, columnCount)
	for col := 0; col < columnCount; col++ {
		b := row[2+col/2]
		if col%2 == 0 {
			nibbles[col] = b & 0x0F
		} else {
			nibbles[col] = (b >> 4) & 0x0F
		}
	}
	return nibbles, payloadStart, nil
}

// ParseCompressedRowLayout parses descriptor nibbles and inline payload slices.
func ParseCompressedRowLayout(row []byte, columns []*schema.Column) ([]CompressedColumnSlice, int, []int, error) {
	nibbles, payloadStart, err := ExtractCompressedColumnNibbles(row, len(columns))
	if err != nil {
		return nil, 0, nil, fmt.Errorf("%w: %v", ErrCompressedRowParseFailed, err)
	}

	slices := make([]CompressedColumnSlice, len(columns))
	currentOffset := payloadStart
	var tailIndexes []int

	for i, col := range columns {
		desc := nibbles[i]
		sl := CompressedColumnSlice{
			Column:     col,
			Descriptor: desc,
		}
		typeName := schema.TypeName(col.TypeID)

		switch desc {
		case 0x0:
			sl.IsNull = true
		case 0x1:
			sl.IsZeroCompressed = true
			sl.LogicalLength = schema.FixedSize(col)
		case 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9:
			sl.PhysicalLength = int(desc) - 1
			if RowCompressionAffectsStorage[typeName] {
				sl.LogicalLength = schema.FixedSize(col)
			} else {
				sl.LogicalLength = sl.PhysicalLength
			}
			sl.Offset = currentOffset
			end := currentOffset + sl.PhysicalLength
			if end > len(row) {
				return nil, 0, nil, fmt.Errorf("%w: inline column %q exceeds row bounds", ErrCompressedRowParseFailed, col.Name)
			}
			sl.CompressedValue = row[currentOffset:end]
			currentOffset = end
		case 0xA:
			sl.IsTailValue = true
			tailIndexes = append(tailIndexes, i)
		case 0xB:
			if col.TypeID != schema.TypeBit {
				sl.IsZeroCompressed = true
				sl.LogicalLength = schema.FixedSize(col)
			}
		case 0xC:
			// Page compression: short anchor/prefix reference.
			// No inline payload bytes. Cannot decode without the compression
			// information record and dictionary stored on the data page.
			sl.IsPageReference = true
		case 0xD:
			// Page compression: long anchor/prefix reference (or extended value marker).
			sl.IsPageReference = true
		default:
			// Unknown descriptor: assume no inline payload bytes; mark null. Do not fail.
			sl.IsNull = true
		}
		slices[i] = sl
	}

	if len(tailIndexes) > 0 {
		slices, err = ParseCompressedTailValues(row, currentOffset, tailIndexes, slices)
		if err != nil {
			return nil, 0, nil, err
		}
	}

	return slices, payloadStart, tailIndexes, nil
}

// ParseCompressedTailValues resolves descriptor 0xA columns using the tail offset array.
func ParseCompressedTailValues(row []byte, start int, tailColumnIndexes []int, slices []CompressedColumnSlice) ([]CompressedColumnSlice, error) {
	headerLen := 2 + len(tailColumnIndexes)*2 + 1
	if start+headerLen > len(row) {
		return nil, fmt.Errorf("%w: tail header exceeds row bounds", ErrCompressedTailParseFailed)
	}

	dataStart := start + headerLen
	prevEnd := 0
	for i, colIdx := range tailColumnIndexes {
		offPos := start + 2 + i*2
		rawOffset := int(binary.BigEndian.Uint16(row[offPos : offPos+2]))
		if rawOffset&0x8000 != 0 {
			rawOffset &= 0x7FFF
		}
		length := rawOffset - prevEnd
		if length < 0 {
			return nil, fmt.Errorf("%w: negative tail length for column %q", ErrCompressedTailParseFailed, slices[colIdx].Column.Name)
		}
		end := dataStart + rawOffset
		begin := end - length
		if begin < dataStart || end > len(row) {
			return nil, fmt.Errorf("%w: tail column %q out of bounds", ErrCompressedTailParseFailed, slices[colIdx].Column.Name)
		}

		sl := slices[colIdx]
		sl.Offset = begin
		sl.PhysicalLength = length
		sl.LogicalLength = length
		sl.CompressedValue = row[begin:end]
		sl.IsTailValue = true
		slices[colIdx] = sl
		prevEnd = rawOffset
	}
	return slices, nil
}

func decodeCompressedBit(sl CompressedColumnSlice) (*rowdecoder.Value, error) {
	switch sl.Descriptor {
	case 0x0:
		return &rowdecoder.Value{IsNull: true}, nil
	case 0xB:
		return &rowdecoder.Value{Raw: true}, nil
	default:
		return &rowdecoder.Value{Raw: false}, nil
	}
}

func decodeCompressedColumnValue(sl CompressedColumnSlice) (interface{}, []byte, error) {
	col := sl.Column
	compressed := sl.CompressedValue
	if sl.IsZeroCompressed {
		compressed = nil
	}

	switch col.TypeID {
	case schema.TypeTinyint:
		if len(compressed) == 0 {
			return int64(0), []byte{0}, nil
		}
		return int64(compressed[0]), compressed, nil

	case schema.TypeSmallint:
		v, raw, err := decodeCompressedInt16(compressed)
		return int64(v), raw, err

	case schema.TypeInt:
		v, raw, err := decodeCompressedInt32(compressed)
		return int64(v), raw, err

	case schema.TypeBigint:
		v, raw, err := decodeCompressedInt64(compressed)
		return v, raw, err

	case schema.TypeSmallmoney:
		v, raw, err := decodeCompressedInt32(compressed)
		if err != nil {
			return nil, nil, err
		}
		return formatMoneyScaled(int64(v)), raw, nil

	case schema.TypeMoney:
		v, raw, err := decodeCompressedInt64(compressed)
		if err != nil {
			return nil, nil, err
		}
		return formatMoneyScaled(v), raw, nil

	case schema.TypeReal:
		v, raw, err := decodeCompressedReal(compressed)
		return v, raw, err

	case schema.TypeFloat:
		fixed := schema.FixedSize(col)
		v, raw, err := decodeCompressedFloat(compressed, fixed)
		return v, raw, err

	case schema.TypeDatetime:
		raw, err := UncompressSignedIntegerLike(compressed, 8)
		if err != nil {
			return nil, nil, err
		}
		return decodeDateTime(raw)

	case schema.TypeSmalldatetime:
		raw, err := UncompressSignedIntegerLike(compressed, 4)
		if err != nil {
			return nil, nil, err
		}
		return decodeSmallDateTime(raw)

	case schema.TypeDate:
		if len(compressed) == 0 {
			return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), make([]byte, 3), nil
		}
		return decodeDate(compressed)

	case schema.TypeTime:
		if len(compressed) == 0 {
			raw := make([]byte, schema.FixedSize(col))
			return decodeTime(raw, col.Scale), raw, nil
		}
		raw := append([]byte(nil), compressed...)
		return decodeTime(raw, col.Scale), raw, nil

	case schema.TypeDatetime2:
		if len(compressed) == 0 {
			raw := make([]byte, schema.FixedSize(col))
			return decodeDateTime2(raw, col.Scale)
		}
		return decodeDateTime2(compressed, col.Scale)

	case schema.TypeDatetimeoffset:
		if len(compressed) == 0 {
			raw := make([]byte, schema.FixedSize(col))
			return decodeDateTimeOffset(raw, col.Scale)
		}
		return decodeDateTimeOffset(compressed, col.Scale)

	case schema.TypeChar:
		raw := append([]byte(nil), compressed...)
		return decodeCompressedCharString(compressed), raw, nil

	case schema.TypeNchar:
		fixed := schema.FixedSize(col)
		s, raw, err := decodeCompressedNCharString(compressed, fixed)
		return s, raw, err

	case schema.TypeBinary:
		fixed := schema.FixedSize(col)
		raw := UncompressBinary(compressed, fixed)
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, raw, nil

	case schema.TypeVarchar, schema.TypeText:
		return decodeCompressedVarChar(compressed, false, col.MaxLength)

	case schema.TypeNvarchar, schema.TypeNtext:
		return decodeCompressedVarChar(compressed, true, col.MaxLength)

	case schema.TypeVarbinary, schema.TypeImage:
		out := append([]byte(nil), compressed...)
		return out, out, nil

	case schema.TypeXML:
		return string(compressed), compressed, nil

	case schema.TypeNumeric, schema.TypeDecimal:
		s, raw, err := decodeVarDecimal(compressed, col.Precision, col.Scale)
		return s, raw, err

	case schema.TypeUniqueidentifier:
		if len(compressed) != 16 {
			return nil, nil, fmt.Errorf("guid compressed length %d", len(compressed))
		}
		s := fmt.Sprintf("%08X-%04X-%04X-%04X-%X",
			binary.LittleEndian.Uint32(compressed[0:4]),
			binary.LittleEndian.Uint16(compressed[4:6]),
			binary.LittleEndian.Uint16(compressed[6:8]),
			binary.BigEndian.Uint16(compressed[8:10]),
			compressed[10:16])
		return s, compressed, nil

	default:
		out := append([]byte(nil), compressed...)
		return out, out, fmt.Errorf("unsupported type_id %d", col.TypeID)
	}
}

func decodeCompressedVarChar(compressed []byte, unicode bool, maxLength int) (interface{}, []byte, error) {
	if len(compressed) == 0 {
		if unicode {
			return "", []byte{}, nil
		}
		return "", []byte{}, nil
	}
	inRow := compressed[0] != 0x00
	payload := compressed
	if !inRow {
		return nil, compressed, fmt.Errorf("off-row varchar not supported in compressed row")
	}
	if unicode {
		expandedLen := maxLength
		if expandedLen < len(payload)*2 {
			expandedLen = len(payload) * 2
		}
		s, raw, err := decodeCompressedNCharString(payload, expandedLen)
		if err != nil {
			return nil, compressed, err
		}
		return strings.TrimRight(s, "\x00"), raw, nil
	}
	return string(payload), payload, nil
}

func decodeVarDecimal(compressed []byte, precision, scale int) (string, []byte, error) {
	if len(compressed) == 0 {
		return decimalZeroString(scale), []byte{}, nil
	}
	positive := compressed[0]&0x80 != 0
	exponent := int(compressed[0]&0x7F) - 64

	var bits strings.Builder
	for _, b := range compressed[1:] {
		fmt.Fprintf(&bits, "%08b", b)
	}
	bitString := bits.String()
	if rem := len(bitString) % 10; rem != 0 {
		bitString += strings.Repeat("0", 10-rem)
	}
	var digits strings.Builder
	for i := 0; i < len(bitString); i += 10 {
		n, err := strconv.ParseUint(bitString[i:i+10], 2, 16)
		if err != nil || n > 999 {
			return "", compressed, fmt.Errorf("invalid vardecimal digit group")
		}
		fmt.Fprintf(&digits, "%03d", n)
	}
	s := strings.TrimLeft(digits.String(), "0")
	if s == "" {
		return decimalZeroString(scale), compressed, nil
	}

	decimalPos := 1 + exponent
	if decimalPos <= 0 {
		s = strings.Repeat("0", 1-decimalPos) + s
		decimalPos = 1
	}
	if decimalPos >= len(s) {
		s += strings.Repeat("0", decimalPos-len(s))
		s += "."
	} else {
		s = s[:decimalPos] + "." + s[decimalPos:]
	}
	parts := strings.SplitN(s, ".", 2)
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if len(frac) < scale {
		frac += strings.Repeat("0", scale-len(frac))
	} else if len(frac) > scale {
		frac = frac[:scale]
	}
	result := parts[0]
	if scale > 0 {
		result += "." + frac
	}
	if !positive && result != decimalZeroString(scale) {
		result = "-" + result
	}
	return result, compressed, nil
}

func decimalZeroString(scale int) string {
	if scale == 0 {
		return "0"
	}
	return "0." + strings.Repeat("0", scale)
}

func formatMoneyScaled(scaled int64) string {
	negative := scaled < 0
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

func decodeDateTime(raw []byte) (time.Time, []byte, error) {
	if len(raw) < 8 {
		return time.Time{}, raw, fmt.Errorf("datetime requires 8 bytes")
	}
	ticks := binary.LittleEndian.Uint32(raw[0:4])
	days := int32(binary.LittleEndian.Uint32(raw[4:8]))
	base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	t := base.AddDate(0, 0, int(days))
	ns := int64(ticks) * int64(time.Second) / 300
	return t.Add(time.Duration(ns)), raw, nil
}

func decodeSmallDateTime(raw []byte) (time.Time, []byte, error) {
	if len(raw) < 4 {
		return time.Time{}, raw, fmt.Errorf("smalldatetime requires 4 bytes")
	}
	days := int(binary.LittleEndian.Uint16(raw[0:2]))
	mins := int(binary.LittleEndian.Uint16(raw[2:4]))
	base := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.AddDate(0, 0, days).Add(time.Duration(mins) * time.Minute), raw, nil
}

func decodeDate(raw []byte) (time.Time, []byte, error) {
	if len(raw) < 3 {
		return time.Time{}, raw, fmt.Errorf("date requires 3 bytes")
	}
	days := int(raw[0]) | int(raw[1])<<8 | int(raw[2])<<16
	base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.AddDate(0, 0, days), raw, nil
}

func decodeTime(raw []byte, scale int) time.Time {
	var ticks int64
	for i, b := range raw {
		ticks |= int64(b) << (8 * i)
	}
	ns := ticks * int64(pow10(9-scale))
	h := ns / int64(time.Hour)
	ns -= h * int64(time.Hour)
	m := ns / int64(time.Minute)
	ns -= m * int64(time.Minute)
	s := ns / int64(time.Second)
	ns -= s * int64(time.Second)
	return time.Date(0, 1, 1, int(h), int(m), int(s), int(ns), time.UTC)
}

func decodeDateTime2(raw []byte, scale int) (time.Time, []byte, error) {
	timeBytes := schema.FixedSize(&schema.Column{TypeID: schema.TypeTime, Scale: scale})
	if len(raw) < timeBytes+3 {
		return time.Time{}, raw, fmt.Errorf("datetime2 too short")
	}
	t := decodeTime(raw[:timeBytes], scale)
	days := int(raw[timeBytes]) | int(raw[timeBytes+1])<<8 | int(raw[timeBytes+2])<<16
	base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)
	return time.Date(base.Year(), base.Month(), base.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC), raw, nil
}

func decodeDateTimeOffset(raw []byte, scale int) (time.Time, []byte, error) {
	timeBytes := schema.FixedSize(&schema.Column{TypeID: schema.TypeTime, Scale: scale})
	if len(raw) < timeBytes+5 {
		return time.Time{}, raw, fmt.Errorf("datetimeoffset too short")
	}
	t := decodeTime(raw[:timeBytes], scale)
	days := int(raw[timeBytes]) | int(raw[timeBytes+1])<<8 | int(raw[timeBytes+2])<<16
	base := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, days)
	offsetMins := int(int16(binary.LittleEndian.Uint16(raw[timeBytes+3:])))
	loc := time.FixedZone("", offsetMins*60)
	return time.Date(base.Year(), base.Month(), base.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc), raw, nil
}

func pow10(n int) int64 {
	p := int64(1)
	for i := 0; i < n; i++ {
		p *= 10
	}
	return p
}

// DecodeDMLRowImage routes row image decoding based on compression metadata.
func DecodeDMLRowImage(
	row []byte,
	table *schema.Table,
	compression CompressionType,
	compressedDecoder CompressedRowDecoder,
) (DecodedRow, error) {
	switch compression {
	case CompressionNone, CompressionColumnstore:
		vals, err := rowdecoder.DecodeRow(row, table)
		if err != nil {
			return DecodedRow{}, err
		}
		return DecodedRow{Values: vals}, nil
	case CompressionRow, CompressionPage:
		if compressedDecoder == nil {
			compressedDecoder = &MSSQLCompressedRowDecoder{}
		}
		return compressedDecoder.Decode(row, table, compression)
	default:
		return DecodedRow{}, ErrCompressionMetadataMissing
	}
}

// MarshalCompressionDebug serializes compression debug info for DuckDB storage.
func MarshalCompressionDebug(info CompressionDebugInfo) string {
	b, err := json.Marshal(info)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// LooksCompressedRow returns true when the row header does not match uncompressed layout.
func LooksCompressedRow(row []byte) bool {
	if len(row) < 4 {
		return true
	}
	foffset := int(binary.LittleEndian.Uint16(row[2:4]))
	return foffset < 4 || foffset > len(row)
}
