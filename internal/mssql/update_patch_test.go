package mssql

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/uns/mssqllogrecovery/internal/logparser"
)

func TestApplyModifyColumns_SQLServerFixture(t *testing.T) {
	before, _ := hex.DecodeString("230FA2AA42158865A716E58A92D644C3F6E8E563552502C2490B80B46700A4CDD8433030314E43303031010203040506666972737401040010001B003C004C0011111111111111111111111111111111706167652D696E736572747061676520636F6D7072657373656420696E736572742061E702F16B6C616D61100102030405060708090A0B0C0D0E0F1000000000000000006D2402000000")
	want, _ := hex.DecodeString("230FA2AA42B68865A516E594D4C978C31BC7A37063552502C2490B80B46700B85894433030314E43303031AA55AA55666972737401040010001C003D004D0011111111111111111111111111111111706167652D757064617465647061676520636F6D70726573736564207570646174652061E702F16B6C616D61100102030405060708090A0B0C0D0E0F1000000000000000006D2402000000")
	c3, _ := hex.DecodeString("0101000C0000B982EE5A00000102000402030004")
	oldFrag := before[5 : 5+108]
	newFrag := want[5 : 5+108]
	raw := append([]byte{}, c3...)
	raw = append(raw, oldFrag...)
	raw = append(raw, 0, 0, 0, 0)
	raw = raw[:len(c3)+align4(len(oldFrag))]
	raw = append(raw, newFrag...)

	rec := logparser.LogRecord{
		Operation:    logparser.OpModifyColumns,
		Contents0:    []byte{0x05, 0x00, 0x05, 0x00},
		Contents1:    []byte{0x6C, 0x00},
		Contents3:    c3,
		RawLogRecord: raw,
	}
	got, err := ApplyModifyColumns(before, rec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("after mismatch\n got=%X\nwant=%X", got, want)
	}
}

func TestReverseModifyColumns(t *testing.T) {
	after := []byte("abcdefghXXklmnop")
	rec := logparser.LogRecord{
		Operation: logparser.OpModifyColumns,
		Contents0: []byte{8, 0, 8, 0},
		Contents1: []byte{2, 0},
		Contents3: []byte{0xaa, 0xbb, 0xcc, 0xdd},
	}
	rec.RawLogRecord = append([]byte{1, 2, 3, 4}, rec.Contents3...)
	rec.RawLogRecord = append(rec.RawLogRecord,
		'Y', 'Y', 0, 0, // aligned old fragment
		'X', 'X', 0xfe, 0xfd, // new fragment followed by unrelated log bytes
	)

	got, err := ReverseModifyColumns(after, rec)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abcdefghYYklmnop" {
		t.Fatalf("before=%q", got)
	}
}

func TestExtractPrimaryKeyProbeFromRowLog2_Prefix16(t *testing.T) {
	// First byte 0x16 → hex "16AABBCC11223344"
	// probe = hex[2 : len-8] = "AABBCC"
	rowLog2 := []byte{0x16, 0xAA, 0xBB, 0xCC, 0x11, 0x22, 0x33, 0x44}
	got := ExtractPrimaryKeyProbeFromRowLog2(rowLog2)
	want := "AABBCC"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExtractPrimaryKeyProbeFromRowLog2_Prefix36(t *testing.T) {
	// First byte 0x36 → hex "3600000000000000ABCD"
	// probe = hex[16:] = "ABCD"
	rowLog2 := []byte{0x36, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAB, 0xCD}
	got := ExtractPrimaryKeyProbeFromRowLog2(rowLog2)
	want := "ABCD"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExtractPrimaryKeyProbeFromRowLog2_Unknown(t *testing.T) {
	rowLog2 := []byte{0xFF, 0x01, 0x02}
	got := ExtractPrimaryKeyProbeFromRowLog2(rowLog2)
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestExtractPrimaryKeyProbeFromRowLog2_Empty(t *testing.T) {
	got := ExtractPrimaryKeyProbeFromRowLog2(nil)
	if got != "" {
		t.Errorf("expected empty string for nil input")
	}
}

func TestPatchBytesReverse_SingleMatch(t *testing.T) {
	after := []byte{0x00, 0x10, 0x20, 0x30, 0x40, 0xFF}
	newFrag := []byte{0x10, 0x20, 0x30, 0x40}
	oldFrag := []byte{0x01, 0x02, 0x03, 0x04}

	before, offset, err := PatchBytesReverse(after, newFrag, oldFrag)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if offset != 1 {
		t.Errorf("offset: got %d, want 1", offset)
	}
	want := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0xFF}
	if len(before) != len(want) {
		t.Fatalf("len(before)=%d, want %d", len(before), len(want))
	}
	for i := range want {
		if before[i] != want[i] {
			t.Errorf("before[%d]=%02X, want %02X", i, before[i], want[i])
		}
	}
	// after must be unchanged.
	if after[1] != 0x10 {
		t.Error("PatchBytesReverse must not modify the original slice")
	}
}

func TestPatchBytesReverse_NotFound(t *testing.T) {
	after := []byte{0x11, 0x22, 0x33}
	newFrag := []byte{0xAA, 0xBB}
	oldFrag := []byte{0x11, 0x22}

	_, _, err := PatchBytesReverse(after, newFrag, oldFrag)
	if !errors.Is(err, ErrPatchNotFound) {
		t.Errorf("got %v, want ErrPatchNotFound", err)
	}
}

func TestPatchBytesReverse_Ambiguous(t *testing.T) {
	after := []byte{0xAA, 0xBB, 0xAA, 0xBB, 0xCC}
	newFrag := []byte{0xAA, 0xBB}
	oldFrag := []byte{0x11, 0x22}

	_, _, err := PatchBytesReverse(after, newFrag, oldFrag)
	if !errors.Is(err, ErrAmbiguousPatch) {
		t.Errorf("got %v, want ErrAmbiguousPatch", err)
	}
}

func TestPatchBytesReverse_VariableLength(t *testing.T) {
	after := []byte{0x00, 0xAA, 0xBB, 0xFF}
	newFrag := []byte{0xAA, 0xBB}
	oldFrag := []byte{0x11} // different length

	_, _, err := PatchBytesReverse(after, newFrag, oldFrag)
	if !errors.Is(err, ErrVariableLengthPatchRequired) {
		t.Errorf("got %v, want ErrVariableLengthPatchRequired", err)
	}
}

func TestPatchBytesReverse_EmptyNewFrag(t *testing.T) {
	after := []byte{0x01, 0x02}
	_, _, err := PatchBytesReverse(after, nil, []byte{0x03})
	if !errors.Is(err, ErrPatchNotFound) {
		t.Errorf("got %v, want ErrPatchNotFound", err)
	}
}
