package mssql

import (
	"encoding/binary"
	"fmt"
	"math"
)

// UncompressSignedIntegerLike expands a row-compressed signed integer payload
// into a fixed-length little-endian byte array.
//
// SQL Server row compression stores integers big-endian with an inverted sign bit:
// MSB of the first byte = 1 means positive, 0 means negative. To uncompress:
// flip the MSB, reverse the bytes to little-endian, sign-extend to fixedLen.
func UncompressSignedIntegerLike(compressed []byte, fixedLen int) ([]byte, error) {
	if len(compressed) == 0 {
		return make([]byte, fixedLen), nil
	}
	if len(compressed) > fixedLen {
		return nil, fmt.Errorf("compressed integer length %d exceeds fixed length %d", len(compressed), fixedLen)
	}

	// MSB of first byte (big-endian order): 1 = positive, 0 = negative.
	positive := compressed[0]&0x80 != 0

	// Flip the sign bit on a copy.
	flipped := make([]byte, len(compressed))
	copy(flipped, compressed)
	if positive {
		flipped[0] &^= 0x80
	} else {
		flipped[0] |= 0x80
	}

	// Build LE output, sign-extending to fixedLen.
	out := make([]byte, fixedLen)
	if !positive {
		for i := range out {
			out[i] = 0xFF
		}
	}
	// Reverse big-endian flipped bytes into little-endian positions.
	for i, b := range flipped {
		out[len(flipped)-1-i] = b
	}
	return out, nil
}

// UncompressFloatLike zero-prefixes a compressed float/real payload to fixedLen.
func UncompressFloatLike(compressed []byte, fixedLen int) ([]byte, error) {
	if len(compressed) == 0 {
		return make([]byte, fixedLen), nil
	}
	if len(compressed) > fixedLen {
		return nil, fmt.Errorf("compressed float length %d exceeds fixed length %d", len(compressed), fixedLen)
	}
	out := make([]byte, fixedLen)
	copy(out[fixedLen-len(compressed):], compressed)
	return out, nil
}

// UncompressBinary right-pads compressed bytes with zeros to fixedLen.
func UncompressBinary(compressed []byte, fixedLen int) []byte {
	out := make([]byte, fixedLen)
	if len(compressed) > fixedLen {
		copy(out, compressed[:fixedLen])
		return out
	}
	copy(out, compressed)
	return out
}

func decodeCompressedInt16(compressed []byte) (int16, []byte, error) {
	raw, err := UncompressSignedIntegerLike(compressed, 2)
	if err != nil {
		return 0, nil, err
	}
	return int16(binary.LittleEndian.Uint16(raw)), raw, nil
}

func decodeCompressedInt32(compressed []byte) (int32, []byte, error) {
	raw, err := UncompressSignedIntegerLike(compressed, 4)
	if err != nil {
		return 0, nil, err
	}
	return int32(binary.LittleEndian.Uint32(raw)), raw, nil
}

func decodeCompressedInt64(compressed []byte) (int64, []byte, error) {
	raw, err := UncompressSignedIntegerLike(compressed, 8)
	if err != nil {
		return 0, nil, err
	}
	return int64(binary.LittleEndian.Uint64(raw)), raw, nil
}

func decodeCompressedReal(compressed []byte) (float64, []byte, error) {
	raw, err := UncompressFloatLike(compressed, 4)
	if err != nil {
		return 0, nil, err
	}
	return float64(math.Float32frombits(binary.LittleEndian.Uint32(raw))), raw, nil
}

func decodeCompressedFloat(compressed []byte, fixedLen int) (float64, []byte, error) {
	raw, err := UncompressFloatLike(compressed, fixedLen)
	if err != nil {
		return 0, nil, err
	}
	return math.Float64frombits(binary.LittleEndian.Uint64(raw)), raw, nil
}
