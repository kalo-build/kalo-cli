package hostfuncs

import (
	"context"
	"time"
)

// wallClockNow returns the current Unix timestamp in nanoseconds.
func wallClockNow() int64 {
	return time.Now().UnixNano()
}

// deterministicNow is stable across hosts and invocations.
func deterministicNow() int64 {
	return 0
}

// systemNow provides clock access to WASM plugins according to host policy.
func (h *KaloHost) systemNow(ctx context.Context) int64 {
	return h.now()
}
