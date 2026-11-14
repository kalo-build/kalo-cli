//go:build unix

package main

import (
	"testing"
	"unsafe"
)

func TestReservedLinearMemoryGrowsInPlace(t *testing.T) {
	memory := newReservedLinearMemory(0, 16*65536)
	defer memory.Free()
	first := memory.Reallocate(65536)
	if len(first) != 65536 {
		t.Fatalf("len = %d", len(first))
	}
	first[65535] = 7
	grown := memory.Reallocate(4 * 65536)
	if unsafe.SliceData(grown) != unsafe.SliceData(first) {
		t.Fatal("growing must not move (and so copy) the buffer")
	}
	if grown[65535] != 7 {
		t.Fatal("contents lost on grow")
	}
	grown[4*65536-1] = 1
	if memory.Reallocate(17*65536) != nil {
		t.Fatal("growing past the reservation must fail so the plugin sees out of memory")
	}
}

func TestReservedLinearMemoryFallsBackWithoutMax(t *testing.T) {
	memory := newReservedLinearMemory(0, 0)
	defer memory.Free()
	buffer := memory.Reallocate(2 * 65536)
	if len(buffer) != 2*65536 {
		t.Fatalf("fallback len = %d", len(buffer))
	}
	buffer[2*65536-1] = 1
}
