package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// hostBaselineMiB is what the host needs next to plugin linear memory: the
// compiled plugin (about 220-275 MiB RSS for the 8.6 MB openapi-sdk-ts
// artifact, measured 14 Nov 2026) plus Go heap headroom.
const hostBaselineMiB = 512

// wasm32 cannot address more than 4 GiB of linear memory.
const wasm32MaxMiB = 4096

type memoryLimit struct {
	MiB    uint32 // zero: no limit (wazero default, 4 GiB)
	Source string // "--plugin-memory-mib" or "free memory"
}

// pluginMemoryLimit returns the per-plugin linear-memory limit. Without
// --plugin-memory-mib it is derived from MemAvailable, so a plugin that
// keeps allocating fails inside the sandbox with an out-of-memory error
// instead of the kernel killing the whole process (task hq-15).
func pluginMemoryLimit(flagMiB uint32) memoryLimit {
	if flagMiB > 0 {
		return memoryLimit{MiB: flagMiB, Source: "--plugin-memory-mib"}
	}
	available, ok := memAvailableMiB("/proc/meminfo")
	if !ok {
		return memoryLimit{}
	}
	return memoryLimit{MiB: defaultPluginMemoryMiB(available), Source: "free memory"}
}

func defaultPluginMemoryMiB(availableMiB uint64) uint32 {
	limit := uint64(128)
	if availableMiB > hostBaselineMiB+limit {
		limit = availableMiB - hostBaselineMiB
	}
	if limit > wasm32MaxMiB {
		limit = wasm32MaxMiB
	}
	return uint32(limit)
}

func memAvailableMiB(path string) (uint64, bool) {
	file, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemAvailable:" {
			kib, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, false
			}
			return kib / 1024, true
		}
	}
	return 0, false
}

// pluginOutOfMemory recognises a plugin that hit its memory limit: Go and
// TinyGo runtimes print "out of memory", Rust prints "memory allocation of N
// bytes failed".
func pluginOutOfMemory(err error, tail string) bool {
	text := strings.ToLower(tail + "\n" + err.Error())
	return strings.Contains(text, "out of memory") || strings.Contains(text, "memory allocation of")
}

func outOfMemoryError(processor, inputPath string, inputBytes int64, limit memoryLimit, quiet bool) error {
	where := "with no memory limit"
	if limit.MiB > 0 {
		where = fmt.Sprintf("at the %d MiB plugin limit (set from %s)", limit.MiB, limit.Source)
	}
	hint := "raise --plugin-memory-mib if the machine has more free memory, or try a smaller input"
	if quiet {
		hint += "; --verbose shows the plugin output"
	}
	return fmt.Errorf("Processor %s ran out of memory %s on %s (%.1f MB): %s",
		processor, where, filepath.Base(inputPath), float64(inputBytes)/1e6, hint)
}
