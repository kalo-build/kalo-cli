package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/kalo-build/kalo-cli/internal/specv0"
)

// stageInputValue places one SPEC value into a plugin's /input directory.
// A file-set value is unpacked; any other value is copied as one file, named
// by KALO:PLUGIN-IO input.filename when the implementation declares one.
func stageInputValue(valuePath string, binding *specv0.Binding, pluginIO *specv0.PluginIO, inputDir string) error {
	if binding != nil && specv0.IsFileSetMediaType(binding.MediaType) {
		data, err := os.ReadFile(valuePath)
		if err != nil {
			return err
		}
		_, err = specv0.UnpackFileSet(data, inputDir)
		return err
	}
	name := filepath.Base(valuePath)
	if pluginIO != nil && pluginIO.Input.Filename != "" {
		name = pluginIO.Input.Filename
	}
	return copyFile(valuePath, filepath.Join(inputDir, name))
}

// packStageOutput packs everything a plugin wrote below outputDir into one
// file-set value at target.
func packStageOutput(outputDir, target string) (string, error) {
	set, err := specv0.PackDirSet(outputDir)
	if err != nil {
		return "", err
	}
	if len(set.Files) == 0 {
		return "", fmt.Errorf("plugin wrote no files")
	}
	data, err := specv0.MarshalFileSet(set)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(target, data, 0o644); err != nil {
		return "", err
	}
	return target, nil
}

// tailWriter forwards everything to dst and keeps the last max bytes.
type tailWriter struct {
	dst  io.Writer
	max  int
	mu   sync.Mutex
	buf  []byte
	head []byte // first headMax bytes: runtimes print the cause before a long dump
}

const headMax = 1024

func (w *tailWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if room := headMax - len(w.head); room > 0 {
		w.head = append(w.head, p[:min(room, len(p))]...)
	}
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		w.buf = append([]byte(nil), w.buf[len(w.buf)-w.max:]...)
	}
	w.mu.Unlock()
	if w.dst != nil {
		_, _ = w.dst.Write(p)
	}
	return len(p), nil
}

func (w *tailWriter) Head() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.head)
}

func (w *tailWriter) Tail() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.buf)
}
