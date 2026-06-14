package mssql

import (
	"os"
	"testing"
)

func TestParseDumpValue(t *testing.T) {
	line, ok := parseDumpValue("0000000000000060: 30001400 01000000 AABBCCDD †0...............")
	if !ok {
		t.Fatal("dump line was not parsed")
	}
	if line.offset != 0x60 {
		t.Fatalf("offset=%d", line.offset)
	}
	if got := stringHex(line.data); got != "3000140001000000aabbccdd" {
		t.Fatalf("data=%s", got)
	}
}

func TestParseDumpSlot(t *testing.T) {
	slot, ok := parseDumpSlot("Slot 12 Offset 0x60 Length 42")
	if !ok || slot != 12 {
		t.Fatalf("slot=%d ok=%v", slot, ok)
	}
}

func TestSQLPageReaderDisabledByDefault(t *testing.T) {
	os.Unsetenv("LOGRECOVERY_ENABLE_PAGE_READER")
	os.Unsetenv("LOGRECOVERY_DISABLE_PAGE_READER")
	if reader := NewSQLPageReader(nil, "db"); !reader.disabled {
		t.Fatal("page reader must be disabled by default")
	}

	t.Setenv("LOGRECOVERY_ENABLE_PAGE_READER", "1")
	if reader := NewSQLPageReader(nil, "db"); reader.disabled {
		t.Fatal("page reader opt-in was ignored")
	}
}

func stringHex(data []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(data)*2)
	for i, b := range data {
		out[i*2] = digits[b>>4]
		out[i*2+1] = digits[b&0xf]
	}
	return string(out)
}
