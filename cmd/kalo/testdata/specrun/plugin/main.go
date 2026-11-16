// Command plugin is a tiny old-ABI Kalo plugin used by the kalo spec run
// tests: it reads /input/<config.specFileName> and writes two files, so the
// test covers YAML input, KALO:PLUGIN-IO config and file-set output packing.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

func main() {
	var args struct {
		Config struct {
			SpecFileName string `json:"specFileName"`
			Fail         bool   `json:"fail"`
		} `json:"config"`
	}
	if len(os.Args) < 2 || json.Unmarshal([]byte(os.Args[1]), &args) != nil {
		fmt.Fprintln(os.Stderr, "fixture plugin: missing config argument")
		os.Exit(2)
	}
	if args.Config.Fail {
		fmt.Fprintln(os.Stderr, "fixture plugin: failing on purpose")
		os.Exit(1)
	}
	spec, err := os.ReadFile("/input/" + args.Config.SpecFileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fixture plugin: %v\n", err)
		os.Exit(1)
	}
	lines := strings.Count(string(spec), "\n")
	if err := os.MkdirAll("/output/src", 0o755); err != nil {
		os.Exit(1)
	}
	_ = os.WriteFile("/output/index.ts", []byte("export * from './src/client';\n"), 0o644)
	_ = os.WriteFile("/output/src/client.ts", []byte(fmt.Sprintf("// generated from %d lines\n", lines)), 0o644)
}
