package mssql

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/uns/mssqllogrecovery/internal/logparser"
	"github.com/uns/mssqllogrecovery/internal/schema"
)

var (
	ErrPatchNotFound               = errors.New("patch fragment not found in after-image")
	ErrAmbiguousPatch              = errors.New("patch fragment found at multiple offsets")
	ErrVariableLengthPatchRequired = errors.New("variable-length patch: old and new fragment lengths differ")
)

// ApplyModifyRowFragment builds the after-image from a cached before-image.
// SQL Server logs Contents0 as the bytes removed from OffsetInRow and Contents1
// as the replacement bytes. The lengths may differ for variable-length updates.
func ApplyModifyRowFragment(before, oldFrag, newFrag []byte, offset int) ([]byte, error) {
	if offset < 0 || offset+len(oldFrag) > len(before) {
		return nil, fmt.Errorf("modify-row fragment outside cached row: offset=%d old=%d row=%d",
			offset, len(oldFrag), len(before))
	}
	if !bytes.Equal(before[offset:offset+len(oldFrag)], oldFrag) {
		return nil, fmt.Errorf("%w: cached row does not contain old fragment at offset %d", ErrPatchNotFound, offset)
	}
	after := make([]byte, 0, len(before)-len(oldFrag)+len(newFrag))
	after = append(after, before[:offset]...)
	after = append(after, newFrag...)
	after = append(after, before[offset+len(oldFrag):]...)
	return after, nil
}

// ApplyModifyColumns reconstructs the after-image for LOP_MODIFY_COLUMNS.
// Contents0 stores pairs of old/new row offsets, Contents1 stores old lengths,
// and the payload following Contents3 in Log Record stores aligned old/new data.
func ApplyModifyColumns(before []byte, rec logparser.LogRecord) ([]byte, error) {
	if len(rec.Contents0) == 0 || len(rec.Contents0)%4 != 0 || len(rec.Contents1) < len(rec.Contents0)/2 {
		return nil, fmt.Errorf("unsupported modify-columns offset/length arrays")
	}
	marker := bytes.Index(rec.RawLogRecord, rec.Contents3)
	if marker < 0 {
		return nil, fmt.Errorf("modify-columns Contents3 marker not found in Log Record")
	}
	payloadStart := marker + len(rec.Contents3)
	if rem := payloadStart % 4; rem != 0 {
		payloadStart += rem
	}
	if payloadStart > len(rec.RawLogRecord) {
		return nil, fmt.Errorf("modify-columns payload outside Log Record")
	}
	payload := rec.RawLogRecord[payloadStart:]

	count := len(rec.Contents0) / 4
	oldStarts := make([]int, count)
	newStarts := make([]int, count)
	oldLens := make([]int, count)
	for i := 0; i < count; i++ {
		oldStarts[i] = int(binary.LittleEndian.Uint16(rec.Contents0[i*4 : i*4+2]))
		newStarts[i] = int(binary.LittleEndian.Uint16(rec.Contents0[i*4+2 : i*4+4]))
		oldLens[i] = int(binary.LittleEndian.Uint16(rec.Contents1[i*2 : i*2+2]))
	}

	out := append([]byte(nil), before...)
	cursor := 0
	shift := 0
	for i := 0; i < count; i++ {
		if cursor+oldLens[i] > len(payload) {
			return nil, fmt.Errorf("modify-columns old fragment %d truncated", i)
		}
		oldFrag := payload[cursor : cursor+oldLens[i]]
		cursor += align4(oldLens[i])
		if cursor > len(payload) {
			return nil, fmt.Errorf("modify-columns old fragment %d alignment exceeds payload", i)
		}

		var newLen int
		if i+1 < count {
			currentShift := newStarts[i] - oldStarts[i]
			nextShift := newStarts[i+1] - oldStarts[i+1]
			newLen = oldLens[i] + nextShift - currentShift
		} else {
			newLen = len(payload) - cursor
		}
		if newLen < 0 || cursor+newLen > len(payload) {
			return nil, fmt.Errorf("modify-columns new fragment %d invalid length %d", i, newLen)
		}
		newFrag := payload[cursor : cursor+newLen]
		cursor += align4(newLen)
		if cursor > len(payload) {
			cursor = len(payload)
		}

		offset := oldStarts[i] + shift
		var err error
		out, err = ApplyModifyRowFragment(out, oldFrag, newFrag, offset)
		if err != nil {
			return nil, err
		}
		shift += len(newFrag) - len(oldFrag)
	}
	return out, nil
}

// ReverseModifyColumns reconstructs the before-image from the current after-image.
// It mirrors SQL Server's old/new offset and aligned fragment layout.
func ReverseModifyColumns(after []byte, rec logparser.LogRecord) ([]byte, error) {
	oldStarts, newStarts, oldLens, payload, err := modifyColumnsParts(rec)
	if err != nil {
		return nil, err
	}
	out := append([]byte(nil), after...)
	cursor := 0
	for i := range oldStarts {
		if cursor+oldLens[i] > len(payload) {
			return nil, fmt.Errorf("modify-columns old fragment %d truncated", i)
		}
		oldFrag := payload[cursor : cursor+oldLens[i]]
		cursor += align4(oldLens[i])
		if cursor > len(payload) {
			return nil, fmt.Errorf("modify-columns old fragment %d alignment exceeds payload", i)
		}

		var newLen int
		if i+1 < len(oldStarts) {
			currentShift := newStarts[i] - oldStarts[i]
			nextShift := newStarts[i+1] - oldStarts[i+1]
			newLen = oldLens[i] + nextShift - currentShift
		} else {
			maxLen := len(payload) - cursor
			if newStarts[i] < 0 || newStarts[i] > len(after) {
				return nil, fmt.Errorf("modify-columns new offset %d outside row", newStarts[i])
			}
			for newLen < maxLen && newStarts[i]+newLen < len(after) &&
				payload[cursor+newLen] == after[newStarts[i]+newLen] {
				newLen++
			}
		}
		if newLen < 0 || cursor+newLen > len(payload) ||
			newStarts[i]+newLen > len(after) {
			return nil, fmt.Errorf("modify-columns new fragment %d invalid length %d", i, newLen)
		}
		newFrag := payload[cursor : cursor+newLen]
		if !bytes.Equal(after[newStarts[i]:newStarts[i]+newLen], newFrag) {
			return nil, fmt.Errorf("%w: current row does not contain new fragment %d", ErrPatchNotFound, i)
		}
		cursor += align4(newLen)

		offset := oldStarts[i]
		if offset < 0 || offset+newLen > len(out) {
			return nil, fmt.Errorf("modify-columns old offset %d outside row", offset)
		}
		next := make([]byte, 0, len(out)-newLen+len(oldFrag))
		next = append(next, out[:offset]...)
		next = append(next, oldFrag...)
		next = append(next, out[offset+newLen:]...)
		out = next
	}
	return out, nil
}

func modifyColumnsParts(rec logparser.LogRecord) ([]int, []int, []int, []byte, error) {
	if len(rec.Contents0) == 0 || len(rec.Contents0)%4 != 0 || len(rec.Contents1) < len(rec.Contents0)/2 {
		return nil, nil, nil, nil, fmt.Errorf("unsupported modify-columns offset/length arrays")
	}
	marker := bytes.Index(rec.RawLogRecord, rec.Contents3)
	if marker < 0 {
		return nil, nil, nil, nil, fmt.Errorf("modify-columns Contents3 marker not found in Log Record")
	}
	// SQL Server advances the row-data start to a 4-byte boundary relative to the
	// log record start. The C# reference trims the post-Contents3 region by
	// (startByteOffset % 4) bytes — not (4 - rem). Match that exactly.
	payloadStart := marker + len(rec.Contents3)
	if rem := payloadStart % 4; rem != 0 {
		payloadStart += rem
	}
	if payloadStart > len(rec.RawLogRecord) {
		return nil, nil, nil, nil, fmt.Errorf("modify-columns payload outside Log Record")
	}
	count := len(rec.Contents0) / 4
	oldStarts := make([]int, count)
	newStarts := make([]int, count)
	oldLens := make([]int, count)
	for i := 0; i < count; i++ {
		oldStarts[i] = int(binary.LittleEndian.Uint16(rec.Contents0[i*4 : i*4+2]))
		newStarts[i] = int(binary.LittleEndian.Uint16(rec.Contents0[i*4+2 : i*4+4]))
		oldLens[i] = int(binary.LittleEndian.Uint16(rec.Contents1[i*2 : i*2+2]))
	}
	return oldStarts, newStarts, oldLens, rec.RawLogRecord[payloadStart:], nil
}

func align4(n int) int {
	if n%4 == 0 {
		return n
	}
	return n + 4 - n%4
}

// ExtractPrimaryKeyProbeFromRowLog2 extracts a best-effort PK probe hex string
// from Contents2. Returns "" when format is unrecognised. Probe-only — not a
// decoded PK value.
func ExtractPrimaryKeyProbeFromRowLog2(rowLog2 []byte) string {
	if len(rowLog2) == 0 {
		return ""
	}
	h := strings.ToUpper(hex.EncodeToString(rowLog2))
	if len(h) < 2 {
		return ""
	}
	switch h[:2] {
	case "16":
		// Skip leading opcode byte (2 hex) and trailing 4-byte metadata (8 hex).
		if len(h) < 10 {
			return ""
		}
		return h[2 : len(h)-8]
	case "36":
		// Skip 8-byte header (16 hex).
		if len(h) < 16 {
			return ""
		}
		return h[16:]
	}
	return ""
}

// PatchBytesReverse reconstructs the before-image (MR0) by locating newFrag
// inside after (MR1) and replacing it with oldFrag.
//
//   - ErrPatchNotFound    — newFrag not found in after, or newFrag is empty
//   - ErrAmbiguousPatch   — newFrag appears at more than one offset
//   - ErrVariableLengthPatchRequired — len(oldFrag) != len(newFrag)
func PatchBytesReverse(after, newFrag, oldFrag []byte) (before []byte, offset int, err error) {
	if len(newFrag) == 0 {
		return nil, 0, fmt.Errorf("%w: empty new fragment", ErrPatchNotFound)
	}

	var offsets []int
	for i := 0; i <= len(after)-len(newFrag); i++ {
		if bytes.Equal(after[i:i+len(newFrag)], newFrag) {
			offsets = append(offsets, i)
		}
	}
	if len(offsets) == 0 {
		return nil, 0, ErrPatchNotFound
	}
	if len(offsets) > 1 {
		return nil, 0, ErrAmbiguousPatch
	}

	offset = offsets[0]
	if len(oldFrag) != len(newFrag) {
		return nil, offset, ErrVariableLengthPatchRequired
	}

	before = make([]byte, len(after))
	copy(before, after)
	copy(before[offset:], oldFrag)
	return before, offset, nil
}

// BuildBeforeImageFromUpdateLog reconstructs MR0 (before-image) by patching
// the new fragment (Contents0) in MR1 with the old fragment (Contents1).
//
// Returns (mr0, patchOffset, err).
// For LOP_MODIFY_COLUMNS, the same fragment-patch is attempted; if it fails
// the caller should record status="unsupported_modify_columns_format".
func BuildBeforeImageFromUpdateLog(rec logparser.LogRecord, after []byte, _ *schema.Table) ([]byte, int, error) {
	switch rec.Operation {
	case logparser.OpModifyRow, logparser.OpModifyColumns:
		newFrag := rec.Contents0
		oldFrag := rec.Contents1
		if len(newFrag) == 0 {
			return nil, 0, fmt.Errorf("%w: Contents0 is empty", ErrPatchNotFound)
		}
		return PatchBytesReverse(after, newFrag, oldFrag)
	default:
		return nil, 0, fmt.Errorf("operation %q is not an update operation", rec.Operation)
	}
}

// ResolveMR1 returns the after-image (MR1) for rec, using the cache first and
// falling back to pageReader. Also returns the source label ("cache" or
// "page_reader") for debug logging.
//
// Cache hit for LOP_MODIFY_ROW is only accepted when Contents0 (the new
// fragment) appears verbatim in the cached image — a staleness guard.
func ResolveMR1(ctx context.Context, rec logparser.LogRecord, pkProbeHex string, cache *RowImageCache, pageReader PageReader) ([]byte, string, error) {
	if rec.PageID != "" && rec.SlotID != nil {
		key := RowImageKey{
			PageID:      rec.PageID,
			SlotID:      *rec.SlotID,
			AllocUnitID: rec.AllocUnitID,
		}
		if entry, ok := cache.Get(key); ok {
			if rec.Operation == logparser.OpModifyColumns {
				return entry.MR1, "cache", nil
			}
			// LOP_MODIFY_ROW: verify after-fragment appears in cached image.
			newFragHex := strings.ToUpper(hex.EncodeToString(rec.Contents0))
			if newFragHex != "" && strings.Contains(entry.MR1Text, newFragHex) {
				return entry.MR1, "cache", nil
			}
		}
	}

	mr1, err := pageReader.ReadCurrentRowImage(ctx, rec, pkProbeHex)
	if err != nil {
		return nil, "", err
	}
	return mr1, "page_reader", nil
}
