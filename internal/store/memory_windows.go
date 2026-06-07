//go:build windows

package store

import (
	"syscall"
	"unsafe"
)

// totalSystemMemoryBytes returns total physical RAM via GlobalMemoryStatusEx.
func totalSystemMemoryBytes() uint64 {
	type memoryStatusEx struct {
		dwLength                uint32
		dwMemoryLoad            uint32
		ullTotalPhys            uint64
		ullAvailPhys            uint64
		ullTotalPageFile        uint64
		ullAvailPageFile        uint64
		ullTotalVirtual         uint64
		ullAvailVirtual         uint64
		ullAvailExtendedVirtual uint64
	}
	var ms memoryStatusEx
	ms.dwLength = uint32(unsafe.Sizeof(ms))
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("GlobalMemoryStatusEx")
	ret, _, _ := proc.Call(uintptr(unsafe.Pointer(&ms)))
	if ret == 0 {
		return 0
	}
	return ms.ullTotalPhys
}
