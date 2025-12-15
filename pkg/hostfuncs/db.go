// Package hostfuncs provides host function implementations for WASM plugins.
package hostfuncs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// Host function error codes
const (
	ErrCodeSuccess            = 0
	ErrCodeStoreNotFound      = 1
	ErrCodeReadNameFailed     = 2
	ErrCodeReadSQLFailed      = 3
	ErrCodeBeginTxFailed      = 4
	ErrCodeMigrationSQLFailed = 5
	ErrCodeRecordFailed       = 6
	ErrCodeCommitFailed       = 7
	ErrCodeCreateTableFailed  = 2
)

// AppliedMigration represents a migration that has been applied to the database.
type AppliedMigration struct {
	Name      string `json:"name"`
	Checksum  string `json:"checksum"`
	AppliedAt int64  `json:"appliedAt"`
}

// DBHostFunctions manages database connections and provides host functions for WASM plugins.
type DBHostFunctions struct {
	connections map[uint32]*pgxpool.Pool
}

// NewDBHostFunctions creates a new DBHostFunctions instance.
func NewDBHostFunctions() *DBHostFunctions {
	return &DBHostFunctions{
		connections: make(map[uint32]*pgxpool.Pool),
	}
}

// AddConnection adds a database connection for a store ID.
func (h *DBHostFunctions) AddConnection(storeID uint32, pool *pgxpool.Pool) {
	h.connections[storeID] = pool
}

// Close closes all database connections.
func (h *DBHostFunctions) Close() {
	for _, pool := range h.connections {
		pool.Close()
	}
}

// Register registers the database host functions with the wazero runtime.
func (h *DBHostFunctions) Register(ctx context.Context, r wazero.Runtime) error {
	_, err := r.NewHostModuleBuilder("kalo").
		NewFunctionBuilder().
		WithFunc(h.dbGetMigrations).
		WithParameterNames("store_id").
		WithResultNames("result_ptr", "result_len").
		Export("db_get_migrations").
		NewFunctionBuilder().
		WithFunc(h.dbApplyMigration).
		WithParameterNames("store_id", "name_ptr", "name_len", "sql_ptr", "sql_len").
		WithResultNames("error_code").
		Export("db_apply_migration").
		NewFunctionBuilder().
		WithFunc(h.dbEnsureTrackingTable).
		WithParameterNames("store_id").
		WithResultNames("error_code").
		Export("db_ensure_tracking_table").
		Instantiate(ctx)

	return err
}

// dbGetMigrations is the host function that returns applied migrations as JSON.
func (h *DBHostFunctions) dbGetMigrations(ctx context.Context, m api.Module, storeID uint32) (uint32, uint32) {
	pool, ok := h.connections[storeID]
	if !ok {
		return 0, 0
	}

	rows, err := pool.Query(ctx, `
		SELECT name, checksum, EXTRACT(EPOCH FROM applied_at)::bigint as applied_at
		FROM kalo_migrations
		ORDER BY name ASC
	`)
	if err != nil {
		// Table might not exist yet, return empty array
		return writeJSONToMemory(ctx, m, []AppliedMigration{})
	}
	defer rows.Close()

	var migrations []AppliedMigration
	for rows.Next() {
		var mig AppliedMigration
		if err := rows.Scan(&mig.Name, &mig.Checksum, &mig.AppliedAt); err != nil {
			continue
		}
		migrations = append(migrations, mig)
	}

	if migrations == nil {
		migrations = []AppliedMigration{}
	}

	return writeJSONToMemory(ctx, m, migrations)
}

// dbApplyMigration is the host function that applies a migration.
func (h *DBHostFunctions) dbApplyMigration(ctx context.Context, m api.Module, storeID, namePtr, nameLen, sqlPtr, sqlLen uint32) uint32 {
	pool, ok := h.connections[storeID]
	if !ok {
		return ErrCodeStoreNotFound
	}

	name, ok := readStringFromMemory(m, namePtr, nameLen)
	if !ok {
		return ErrCodeReadNameFailed
	}

	sql, ok := readBytesFromMemory(m, sqlPtr, sqlLen)
	if !ok {
		return ErrCodeReadSQLFailed
	}

	checksum := ComputeChecksum(sql)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return ErrCodeBeginTxFailed
	}
	defer tx.Rollback(ctx)

	if _, err = tx.Exec(ctx, string(sql)); err != nil {
		return ErrCodeMigrationSQLFailed
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO kalo_migrations (name, checksum, applied_at)
		VALUES ($1, $2, $3)
	`, name, checksum, time.Now())
	if err != nil {
		return ErrCodeRecordFailed
	}

	if err := tx.Commit(ctx); err != nil {
		return ErrCodeCommitFailed
	}

	return ErrCodeSuccess
}

// dbEnsureTrackingTable is the host function that creates the migration tracking table.
func (h *DBHostFunctions) dbEnsureTrackingTable(ctx context.Context, m api.Module, storeID uint32) uint32 {
	pool, ok := h.connections[storeID]
	if !ok {
		return ErrCodeStoreNotFound
	}

	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS kalo_migrations (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			checksum TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return ErrCodeCreateTableFailed
	}

	return ErrCodeSuccess
}

// writeJSONToMemory marshals data to JSON and writes it to WASM memory.
// It calls the WASM module's exported kalo_alloc function to allocate memory.
// If kalo_alloc is not exported, falls back to a fixed buffer region.
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

// readStringFromMemory reads a string from WASM memory.
func readStringFromMemory(m api.Module, ptr, length uint32) (string, bool) {
	if length == 0 {
		return "", true
	}

	mem := m.Memory()
	if mem == nil {
		return "", false
	}

	bytes, ok := mem.Read(ptr, length)
	if !ok {
		return "", false
	}

	return string(bytes), true
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

// ComputeChecksum computes a SHA256 checksum of the given data.
func ComputeChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}
