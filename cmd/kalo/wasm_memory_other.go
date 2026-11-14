//go:build !unix

package main

import "context"

// withReservedLinearMemory keeps wazero's default slice-backed linear memory
// where no mmap reservation is available.
func withReservedLinearMemory(ctx context.Context) context.Context { return ctx }
