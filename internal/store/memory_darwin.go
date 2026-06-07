//go:build darwin

package store

import (
	"syscall"
	"unsafe"
)

func totalSystemMemoryBytes() uint64 {
	s, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0
	}
	// sysctl returns a raw byte string for hw.memsize; reinterpret as uint64.
	b := []byte(s)
	if len(b) < 8 {
		return 0
	}
	return *(*uint64)(unsafe.Pointer(&b[0]))
}
