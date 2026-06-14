package main

import "testing"

func TestIncludeHistoricalForSince(t *testing.T) {
	tests := []struct {
		since string
		want  bool
	}{
		{since: "now", want: false},
		{since: " NOW ", want: false},
		{since: "24h", want: true},
		{since: "7d", want: true},
		{since: "all", want: true},
	}
	for _, tt := range tests {
		if got := includeHistoricalForSince(tt.since); got != tt.want {
			t.Errorf("includeHistoricalForSince(%q)=%v want %v", tt.since, got, tt.want)
		}
	}
}
