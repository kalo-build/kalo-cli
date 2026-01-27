// Package hostfuncs provides host function implementations for WASM plugins.
// These functions are exported to plugins via the "kalo" host module.
package hostfuncs

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// Host function error codes
const (
	ErrCodeSuccess       = 0
	ErrCodeStoreNotFound = 1
	ErrCodeReadSQLFailed = 2
	ErrCodeExecFailed    = 3
	ErrCodeQueryFailed   = 4
)

// KaloHost manages host capabilities and provides host functions for WASM plugins.
// It is responsible for registering all host functions with the "kalo" module.
type KaloHost struct {
	connections map[uint32]*pgxpool.Pool
}

// NewKaloHost creates a new KaloHost instance.
func NewKaloHost() *KaloHost {
	return &KaloHost{
		connections: make(map[uint32]*pgxpool.Pool),
	}
}

// AddConnection adds a database connection for a store ID.
func (h *KaloHost) AddConnection(storeID uint32, pool *pgxpool.Pool) {
	h.connections[storeID] = pool
}

// Close closes all database connections.
func (h *KaloHost) Close() {
	for _, pool := range h.connections {
		pool.Close()
	}
}

// Register registers all host functions with the wazero runtime.
// This creates the "kalo" host module with database and system capabilities.
func (h *KaloHost) Register(ctx context.Context, r wazero.Runtime) error {
	_, err := r.NewHostModuleBuilder("kalo").
		// Database functions
		NewFunctionBuilder().
		WithFunc(h.dbExec).
		WithParameterNames("store_id", "sql_ptr", "sql_len").
		WithResultNames("error_code").
		Export("db_exec").
		NewFunctionBuilder().
		WithFunc(h.dbQuery).
		WithParameterNames("store_id", "sql_ptr", "sql_len").
		WithResultNames("packed_result").
		Export("db_query").
		// System capability functions
		NewFunctionBuilder().
		WithFunc(systemNow).
		WithResultNames("unix_nanos").
		Export("system_now").
		Instantiate(ctx)

	return err
}

// writeJSONToMemory marshals data to JSON and writes it to WASM memory.
// Returns (ptr, length) or (0, 0) on failure.
func writeJSONToMemory(ctx context.Context, m api.Module, data interface{}) (uint32, uint32) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return 0, 0
	}

	mem := m.Memory()
	if mem == nil {
		return 0, 0
	}

	size := uint32(len(jsonBytes))

	// Try to use the SDK's exported allocator
	var ptr uint32
	if allocFn := m.ExportedFunction("kalo_alloc"); allocFn != nil {
		results, err := allocFn.Call(ctx, uint64(size))
		if err == nil && len(results) > 0 && results[0] != 0 {
			ptr = uint32(results[0])
		}
	}

	// Fallback: use fixed buffer region if allocator not available
	if ptr == 0 {
		const bufferOffset = uint32(0x100000) // 1MB
		ptr = bufferOffset
		endAddr := ptr + size

		// Ensure memory is large enough
		currentSize := mem.Size()
		if endAddr > currentSize {
			pagesToGrow := (endAddr - currentSize + 65535) / 65536
			if _, ok := mem.Grow(pagesToGrow); !ok {
				return 0, 0
			}
		}
	}

	if !mem.Write(ptr, jsonBytes) {
		return 0, 0
	}

	return ptr, size
}

// readBytesFromMemory reads bytes from WASM memory.
func readBytesFromMemory(m api.Module, ptr, length uint32) ([]byte, bool) {
	if length == 0 {
		return []byte{}, true
	}

	mem := m.Memory()
	if mem == nil {
		return nil, false
	}

	return mem.Read(ptr, length)
}
