package main

import (
	"bytes"
	"io"
	"sync"
)

// warningWriter passes on only the lines a plugin starts with "warning:", so a
// quiet recipe run still tells the user about lossy input (for example an
// OpenAPI 3.1 file read as 3.0). Each forwarded line is prefixed with the
// processor that wrote it. Lines longer than warningLineMax are dropped.
type warningWriter struct {
	dst    io.Writer
	prefix string
	mu     sync.Mutex
	line   []byte
	tooBig bool
}

const warningLineMax = 1024

var warningMarker = []byte("warning:")

func (w *warningWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		chunk := p
		if i >= 0 {
			chunk = p[:i]
		}
		if !w.tooBig {
			if len(w.line)+len(chunk) > warningLineMax {
				w.tooBig, w.line = true, w.line[:0]
			} else {
				w.line = append(w.line, chunk...)
			}
		}
		if i < 0 {
			break
		}
		w.emit()
		p = p[i+1:]
	}
	return n, nil
}

// Flush emits a last warning line that had no trailing newline.
func (w *warningWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.emit()
}

func (w *warningWriter) emit() {
	line := bytes.TrimRight(w.line, "\r")
	if !w.tooBig && bytes.HasPrefix(line, warningMarker) {
		_, _ = io.WriteString(w.dst, w.prefix+string(line)+"\n")
	}
	w.line, w.tooBig = w.line[:0], false
}
