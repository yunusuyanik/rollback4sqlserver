package mssql

import (
	"encoding/hex"
	"strings"
	"sync"
)

// RowImageKey identifies a row by its storage location.
type RowImageKey struct {
	PageID      string
	SlotID      int
	AllocUnitID int64
}

// RowImageCacheEntry holds the after-image for one row.
type RowImageCacheEntry struct {
	MR1     []byte
	MR1Text string // upper-case hex of MR1, used for fragment verification
}

// RowImageCache is a concurrent map of page/slot/allocUnit → row after-image.
// Populated externally (e.g. from prior full-image log records or page reads).
type RowImageCache struct {
	mu       sync.RWMutex
	items    map[RowImageKey]RowImageCacheEntry
	maxItems int
}

func NewRowImageCache() *RowImageCache {
	// Bounded so the persistent (cross-poll) cache can't grow without limit on a
	// hot table. Each entry is ~1 KB; 500k ≈ a few hundred MB worst case. When
	// exceeded the cache is cleared — warm rows fall back to DBCC/forward re-seed.
	return &RowImageCache{items: make(map[RowImageKey]RowImageCacheEntry), maxItems: 500000}
}

func (c *RowImageCache) Get(key RowImageKey) (RowImageCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.items[key]
	return e, ok
}

func (c *RowImageCache) Set(key RowImageKey, mr1 []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxItems > 0 && len(c.items) >= c.maxItems {
		if _, exists := c.items[key]; !exists {
			c.items = make(map[RowImageKey]RowImageCacheEntry)
		}
	}
	c.items[key] = RowImageCacheEntry{
		MR1:     mr1,
		MR1Text: strings.ToUpper(hex.EncodeToString(mr1)),
	}
}

func (c *RowImageCache) Delete(key RowImageKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *RowImageCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
