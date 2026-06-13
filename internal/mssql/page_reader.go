package mssql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/uns/mssqllogrecovery/internal/logparser"
)

// ErrMR1NotFound is returned when the after-image cannot be resolved from
// cache or page reader.
var ErrMR1NotFound = errors.New("MR1 row image not found")

// PageReader retrieves the current on-disk row image for a given log record.
// Implementations may read from DBCC PAGE output, a live table query, or any
// other source. The pkProbeHex is a best-effort primary key hint extracted from
// RowLog Contents 2 — it may be empty.
type PageReader interface {
	ReadCurrentRowImage(ctx context.Context, rec logparser.LogRecord, pkProbeHex string) ([]byte, error)
}

// NopPageReader always returns ErrMR1NotFound.
// Use as a placeholder when no page reader is available.
type NopPageReader struct{}

func (NopPageReader) ReadCurrentRowImage(_ context.Context, _ logparser.LogRecord, _ string) ([]byte, error) {
	return nil, ErrMR1NotFound
}

// SQLPageReader reads and caches slot images from DBCC PAGE.
type SQLPageReader struct {
	db       *sql.DB
	database string
	mu       sync.Mutex
	pages    map[string]map[int][]byte
}

func NewSQLPageReader(db *sql.DB, database string) *SQLPageReader {
	return &SQLPageReader{db: db, database: database, pages: make(map[string]map[int][]byte)}
}

func (r *SQLPageReader) ReadCurrentRowImage(ctx context.Context, rec logparser.LogRecord, _ string) ([]byte, error) {
	if r == nil || r.db == nil || rec.PageID == "" || rec.SlotID == nil {
		return nil, ErrMR1NotFound
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if slots, ok := r.pages[rec.PageID]; ok {
		if row := slots[*rec.SlotID]; len(row) > 0 {
			return append([]byte(nil), row...), nil
		}
		return nil, ErrMR1NotFound
	}
	slots, err := r.readPage(ctx, rec.PageID)
	if err != nil {
		return nil, err
	}
	r.pages[rec.PageID] = slots
	row := slots[*rec.SlotID]
	if len(row) == 0 {
		return nil, ErrMR1NotFound
	}
	return append([]byte(nil), row...), nil
}

type pageDumpLine struct {
	offset int64
	data   []byte
}

func (r *SQLPageReader) readPage(ctx context.Context, pageID string) (map[int][]byte, error) {
	parts := strings.Split(pageID, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid page id %q", pageID)
	}
	fileID, err := strconv.ParseInt(parts[0], 16, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid page file id %q: %w", parts[0], err)
	}
	pageNum, err := strconv.ParseInt(parts[1], 16, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid page number %q: %w", parts[1], err)
	}
	dbName := strings.ReplaceAll(r.database, "]", "]]")
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(
		"DBCC PAGE([%s], %d, %d, 3) WITH TABLERESULTS, NO_INFOMSGS",
		dbName, fileID, pageNum))
	if err != nil {
		return nil, fmt.Errorf("dbcc page %s: %w", pageID, err)
	}
	defer rows.Close()

	lines := make(map[int][]pageDumpLine)
	for rows.Next() {
		var parent, object, field, value sql.NullString
		if err := rows.Scan(&parent, &object, &field, &value); err != nil {
			return nil, err
		}
		if !strings.Contains(object.String, "Memory Dump") {
			continue
		}
		slot, ok := parseDumpSlot(parent.String)
		if !ok {
			continue
		}
		line, ok := parseDumpValue(value.String)
		if ok {
			lines[slot] = append(lines[slot], line)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slots := make(map[int][]byte, len(lines))
	for slot, dumpLines := range lines {
		sort.Slice(dumpLines, func(i, j int) bool { return dumpLines[i].offset < dumpLines[j].offset })
		for _, line := range dumpLines {
			slots[slot] = append(slots[slot], line.data...)
		}
	}
	return slots, nil
}

func parseDumpSlot(parent string) (int, bool) {
	if !strings.HasPrefix(parent, "Slot ") {
		return 0, false
	}
	rest := strings.TrimPrefix(parent, "Slot ")
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		return 0, false
	}
	slot, err := strconv.Atoi(rest[:end])
	return slot, err == nil
}

func parseDumpValue(value string) (pageDumpLine, bool) {
	colon := strings.IndexByte(value, ':')
	if colon < 0 {
		return pageDumpLine{}, false
	}
	address := strings.TrimSpace(value[:colon])
	offset, err := strconv.ParseInt(strings.TrimPrefix(address, "0x"), 16, 64)
	if err != nil {
		return pageDumpLine{}, false
	}
	payload := value[colon+1:]
	if end := strings.IndexRune(payload, '†'); end >= 0 {
		payload = payload[:end]
	}
	var hexText strings.Builder
	for _, field := range strings.Fields(payload) {
		if len(field)%2 != 0 {
			break
		}
		if _, err := hex.DecodeString(field); err != nil {
			break
		}
		hexText.WriteString(field)
	}
	data, err := hex.DecodeString(hexText.String())
	return pageDumpLine{offset: offset, data: data}, err == nil && len(data) > 0
}
