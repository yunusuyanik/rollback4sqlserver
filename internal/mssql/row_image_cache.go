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
	mu    sync.RWMutex
	items map[RowImageKey]RowImageCacheEntry
}

func NewRowImageCache() *RowImageCache {
	return &RowImageCache{items: make(map[RowImageKey]RowImageCacheEntry)}
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
