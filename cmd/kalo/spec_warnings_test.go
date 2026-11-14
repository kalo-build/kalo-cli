package main

import (
	"strings"
	"testing"
)

func TestWarningWriterForwardsOnlyWarningLines(t *testing.T) {
	var out strings.Builder
	w := &warningWriter{dst: &out, prefix: "openapi-sdk-ts: "}
	// Split writes, CRLF, a noisy line, a warning without a trailing newline.
	for _, s := range []string{"[INFO] reading spec\nwarn", "ing: openapi 3.1 input\r\n", "  warning: indented is not a warning\n", "warning: last"} {
		_, _ = w.Write([]byte(s))
	}
	w.Flush()
	want := "openapi-sdk-ts: warning: openapi 3.1 input\nopenapi-sdk-ts: warning: last\n"
	if out.String() != want {
		t.Fatalf("got %q, want %q", out.String(), want)
	}
}

func TestWarningWriterDropsOverlongLines(t *testing.T) {
	var out strings.Builder
	w := &warningWriter{dst: &out}
	_, _ = w.Write([]byte("warning: " + strings.Repeat("x", 2*warningLineMax) + "\nwarning: ok\n"))
	if out.String() != "warning: ok\n" {
		t.Fatalf("got %q", out.String())
	}
}
