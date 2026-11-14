//go:build unix

package main

import (
	"context"
	"syscall"

	"github.com/tetratelabs/wazero/experimental"
)

// withReservedLinearMemory makes every module instantiated with the returned
// context back its linear memory with one address-space reservation instead
// of a Go slice. wazero's default grows memory with append: each memory.grow
// copies the whole buffer and leaves the old one to the garbage collector, so
// a plugin using N MiB could hold 2-3 N MiB of RSS (task hq-15: an 8 MB
// OpenAPI input passed 900 MB RSS with a 512 MiB plugin cap). Here the
// reservation is PROT_NONE (no RSS, no commit charge) and growing only
// changes protection, so RSS follows the pages the plugin actually touches.
func withReservedLinearMemory(ctx context.Context) context.Context {
	return experimental.WithMemoryAllocator(ctx, experimental.MemoryAllocatorFunc(newReservedLinearMemory))
}

type reservedLinearMemory struct {
	reserved  []byte
	committed int
	fallback  []byte
}

func newReservedLinearMemory(_, max uint64) experimental.LinearMemory {
	return &reservedLinearMemory{reserved: reserveAddressSpace(max)}
}

func reserveAddressSpace(max uint64) []byte {
	if max == 0 || max > 1<<32 || uint64(int(max)) != max {
		return nil
	}
	buf, err := syscall.Mmap(-1, 0, int(max), syscall.PROT_NONE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		return nil
	}
	return buf
}

// Reallocate returns nil when the size is past the reservation or the kernel
// refuses the commit; the plugin then sees memory.grow fail (out of memory)
// instead of the host being killed.
func (m *reservedLinearMemory) Reallocate(size uint64) []byte {
	if m.reserved == nil {
		// No reservation (unbounded max or mmap refused): wazero's default.
		if extra := int(size) - len(m.fallback); extra > 0 {
			m.fallback = append(m.fallback, make([]byte, extra)...)
		}
		return m.fallback[:size]
	}
	if size > uint64(len(m.reserved)) {
		return nil
	}
	if int(size) > m.committed {
		if err := syscall.Mprotect(m.reserved[m.committed:size], syscall.PROT_READ|syscall.PROT_WRITE); err != nil {
			return nil
		}
		m.committed = int(size)
	}
	return m.reserved[:size:size]
}

func (m *reservedLinearMemory) Free() {
	if m.reserved != nil {
		_ = syscall.Munmap(m.reserved)
		m.reserved, m.committed = nil, 0
	}
}
