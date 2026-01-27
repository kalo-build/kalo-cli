package hostfuncs

import (
	"context"
	"time"
)

// systemNow returns the current Unix timestamp in nanoseconds.
// This provides real-time clock access for WASM plugins since WASI
// environments typically don't have access to the real-time clock.
func systemNow(ctx context.Context) int64 {
	return time.Now().UnixNano()
}
