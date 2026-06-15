package logparser

import (
	"strings"
	"testing"
)

func TestBuildLiveChunkQueryUsesLSNCursorAndLimit(t *testing.T) {
	query := buildLiveChunkQuery("000000CE:00000418:0010", 5000)

	for _, want := range []string{
		"SELECT TOP (5000)",
		"fn_dblog(N'000000CE:00000418:0010',NULL)",
		"[Current LSN]>N'000000CE:00000418:0010'",
		"ORDER BY [Current LSN]",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q: %s", want, query)
		}
	}
}

func TestBuildLiveQueryWithoutCursorRemainsUnbounded(t *testing.T) {
	query := buildLiveQuery("")
	if strings.Contains(query, "TOP (") {
		t.Fatalf("compat query unexpectedly paginated: %s", query)
	}
	if !strings.Contains(query, "fn_dblog(NULL,NULL)") {
		t.Fatalf("query does not start at active-log head: %s", query)
	}
}
