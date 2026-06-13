package mssql

import (
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf16"
)

// UncompressNChar expands a row-compressed nchar payload to UTF-16LE fixed width.
func UncompressNChar(compressed []byte, fixedLen int) ([]byte, error) {
	if len(compressed) == 0 {
		return make([]byte, fixedLen), nil
	}
	out := make([]byte, 0, fixedLen)
	for i := 0; i < len(compressed); i++ {
		current := compressed[i]
		var next byte
		hasNext := i+1 < len(compressed)
		if hasNext {
			next = compressed[i+1]
		}

		if current == 0x10 && i == len(compressed)-1 {
			break
		}
		if current != 0x00 && hasNext && next == 0x00 {
			out = append(out, current, next)
			i++
			continue
		}
		if current == 0x0E {
			if i+2 >= len(compressed) {
				return nil, fmt.Errorf("truncated nchar 0x0E marker")
			}
			out = append(out, compressed[i+2], compressed[i+1])
			i += 2
			continue
		}
		if current >= 0x01 && current <= 0x0D && hasNext && next >= 0x80 {
			out = append(out, next^0xC0, current-1)
			i++
			continue
		}
		out = append(out, current, 0x00)
	}

	if len(out) > fixedLen {
		out = out[:fixedLen]
	}
	if len(out) < fixedLen {
		padded := make([]byte, fixedLen)
		copy(padded, out)
		out = padded
	}
	return out, nil
}

func decodeCompressedNCharString(compressed []byte, fixedLen int) (string, []byte, error) {
	raw, err := UncompressNChar(compressed, fixedLen)
	if err != nil {
		return "", nil, err
	}
	u16 := make([]uint16, len(raw)/2)
	for i := range u16 {
		u16[i] = binary.LittleEndian.Uint16(raw[2*i : 2*i+2])
	}
	return strings.TrimRight(string(utf16.Decode(u16)), " \x00"), raw, nil
}

func decodeCompressedCharString(compressed []byte) string {
	return strings.TrimRight(string(compressed), " ")
}
