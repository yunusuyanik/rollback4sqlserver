package logparser

import (
	"database/sql"
	"strings"
	"testing"
)

func TestBuildDumpQuery_S3UsesURLAndAllBackupSlots(t *testing.T) {
	query := buildDumpQuery("", "", []string{"s3://bucket/path/log.trn"}, nil)

	if !strings.Contains(query, "fn_dump_dblog(NULL,NULL,N'URL',1,N's3://bucket/path/log.trn'") {
		t.Fatalf("query does not use URL device: %s", query)
	}
	if got := strings.Count(query, ",DEFAULT"); got != 63 {
		t.Fatalf("DEFAULT slot count=%d want 63", got)
	}
	if !strings.Contains(query, "[Transaction Name]") {
		t.Fatalf("query does not select transaction name: %s", query)
	}
}

func TestBuildDumpQuery_LocalFileUsesDisk(t *testing.T) {
	query := buildDumpQuery("", "", []string{`C:\backups\log.trn`}, nil)
	if !strings.Contains(query, "fn_dump_dblog(NULL,NULL,N'DISK',1,") {
		t.Fatalf("query does not use DISK device: %s", query)
	}
}

func TestTRNReader_URLFilesAreSequentialBackupSets(t *testing.T) {
	reader := NewTRNReader((*sql.DB)(nil), []string{
		"s3://bucket/log-1.trn",
		"s3://bucket/log-2.trn",
	})
	if got := dumpDeviceType(reader.files); got != "URL" {
		t.Fatalf("device=%s", got)
	}
}

func TestBuildDumpChunkQueryUsesLSNCursorAndLimit(t *testing.T) {
	query := buildDumpChunkQuery(
		"000000CE:00000418:0010",
		"",
		[]string{`C:\backups\log.trn`},
		[]string{OpBeginXact, OpCommitXact},
		5000,
	)
	for _, want := range []string{
		"SELECT TOP (5000)",
		"fn_dump_dblog(N'000000CE:00000418:0010',NULL",
		"[Current LSN]>N'000000CE:00000418:0010'",
		"ORDER BY [Current LSN]",
	} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q: %s", want, query)
		}
	}
}
