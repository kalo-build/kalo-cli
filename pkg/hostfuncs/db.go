package hostfuncs

import (
	"context"
	"log"

	"github.com/tetratelabs/wazero/api"
)

// dbExec is the host function that executes SQL that doesn't return rows.
// Returns 0 on success, non-zero error code on failure.
func (h *KaloHost) dbExec(ctx context.Context, m api.Module, storeID, sqlPtr, sqlLen uint32) uint32 {
	pool, ok := h.connections[storeID]
	if !ok {
		log.Printf("[hostfuncs] ERROR: store ID %d not found", storeID)
		return ErrCodeStoreNotFound
	}

	sql, ok := readBytesFromMemory(m, sqlPtr, sqlLen)
	if !ok {
		log.Printf("[hostfuncs] ERROR: failed to read SQL from memory")
		return ErrCodeReadSQLFailed
	}

	log.Printf("[hostfuncs] Executing SQL (%d bytes)", len(sql))

	_, err := pool.Exec(ctx, string(sql))
	if err != nil {
		log.Printf("[hostfuncs] ERROR: SQL execution failed: %v", err)
		return ErrCodeExecFailed
	}

	log.Printf("[hostfuncs] SQL executed successfully")
	return ErrCodeSuccess
}

// dbQuery is the host function that executes SQL that returns rows.
// Returns a packed uint64: high 32 bits = pointer, low 32 bits = (length << 8 | errCode)
func (h *KaloHost) dbQuery(ctx context.Context, m api.Module, storeID, sqlPtr, sqlLen uint32) uint64 {
	pool, ok := h.connections[storeID]
	if !ok {
		log.Printf("[hostfuncs] ERROR: store ID %d not found", storeID)
		return packQueryResult(0, 0, ErrCodeStoreNotFound)
	}

	sql, ok := readBytesFromMemory(m, sqlPtr, sqlLen)
	if !ok {
		log.Printf("[hostfuncs] ERROR: failed to read SQL from memory")
		return packQueryResult(0, 0, ErrCodeReadSQLFailed)
	}

	log.Printf("[hostfuncs] Querying SQL (%d bytes)", len(sql))

	rows, err := pool.Query(ctx, string(sql))
	if err != nil {
		log.Printf("[hostfuncs] ERROR: SQL query failed: %v", err)
		return packQueryResult(0, 0, ErrCodeQueryFailed)
	}
	defer rows.Close()

	// Convert rows to JSON array of objects
	var results []map[string]interface{}
	fieldDescriptions := rows.FieldDescriptions()

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, fd := range fieldDescriptions {
			if i < len(values) {
				row[string(fd.Name)] = values[i]
			}
		}
		results = append(results, row)
	}

	if results == nil {
		results = []map[string]interface{}{}
	}

	// Write result to WASM memory
	ptr, length := writeJSONToMemory(ctx, m, results)
	if ptr == 0 {
		return packQueryResult(0, 0, ErrCodeQueryFailed)
	}

	log.Printf("[hostfuncs] Query returned %d rows", len(results))
	return packQueryResult(ptr, length, ErrCodeSuccess)
}

// packQueryResult packs query result into a uint64.
// Format: high 32 bits = ptr, low 32 bits = (length << 8 | errCode)
func packQueryResult(ptr, length, errCode uint32) uint64 {
	lenAndErr := (length << 8) | (errCode & 0xFF)
	return (uint64(ptr) << 32) | uint64(lenAndErr)
}
