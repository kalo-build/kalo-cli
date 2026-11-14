package specv0

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// MediaTypeFileSet is the Kalo representation of several files as one SPEC
// value. It is a Kalo convention carried by a Representation Binding's
// media_type; SPEC v0.1-alpha itself is unchanged.
const MediaTypeFileSet = "application/vnd.kalo.fileset+json"

func normaliseMediaType(mediaType string) string {
	return strings.ToLower(strings.TrimSpace(strings.SplitN(mediaType, ";", 2)[0]))
}

// IsFileSetMediaType reports whether values of a binding are Kalo file sets,
// ignoring case and media type parameters.
func IsFileSetMediaType(mediaType string) bool {
	return normaliseMediaType(mediaType) == MediaTypeFileSet
}

// IsYAMLMediaType reports whether values of a binding are YAML documents.
func IsYAMLMediaType(mediaType string) bool {
	mediaType = normaliseMediaType(mediaType)
	switch mediaType {
	case "application/yaml", "application/x-yaml", "text/yaml", "text/x-yaml":
		return true
	}
	return strings.HasSuffix(mediaType, "+yaml")
}

// DecodeValue decodes a represented value by its binding media type. YAML is
// normalised through JSON so JSON Schema validation sees the same types for a
// YAML document as for the equivalent JSON document (so an unquoted
// 2020-01-01 is validated as a timestamp string; the plugin still gets the raw
// bytes). A YAML value must be exactly one document. Digests are always taken
// over the raw bytes, never over the decoded value.
func DecodeValue(mediaType string, data []byte) (any, error) {
	if IsYAMLMediaType(mediaType) {
		var raw any
		decoder := yaml.NewDecoder(bytes.NewReader(data))
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("decode YAML: %w", err)
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			if err != nil {
				return nil, fmt.Errorf("decode YAML: %w", err)
			}
			return nil, fmt.Errorf("decode YAML: value must be a single YAML document")
		}
		normalised, err := json.Marshal(raw)
		if err != nil {
			return nil, fmt.Errorf("YAML value is not representable as JSON: %w", err)
		}
		data = normalised
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

// FileSet is the decoded form of an application/vnd.kalo.fileset+json value.
type FileSet struct {
	Files []FileSetEntry `json:"files"`
}

type FileSetEntry struct {
	Path     string `json:"path"`
	SHA256   string `json:"sha256"`
	Encoding string `json:"encoding"`
	Content  string `json:"content"`
}

// PackDir packs every regular file below root into a file set, sorted by
// slash-separated relative path. Symlinks and other special files are refused.
func PackDir(root string) ([]byte, error) {
	set, err := PackDirSet(root)
	if err != nil {
		return nil, err
	}
	return MarshalFileSet(set)
}

// MarshalFileSet writes the canonical encoding of a file set.
func MarshalFileSet(set FileSet) ([]byte, error) {
	data, err := json.MarshalIndent(set, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// PackDirSet is PackDir without the encoding step. Every path is checked with
// the rules UnpackFileSet applies, so a packed set can always be unpacked.
func PackDirSet(root string) (FileSet, error) {
	set := FileSet{Files: []FileSetEntry{}}
	err := filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("file set: %s is not a regular file", current)
		}
		rel, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		slashRel := filepath.ToSlash(rel)
		if !utf8.ValidString(slashRel) {
			return fmt.Errorf("file set: path %q is not valid UTF-8", slashRel)
		}
		if _, err := cleanFileSetPath(slashRel); err != nil {
			return err
		}
		data, err := os.ReadFile(current)
		if err != nil {
			return err
		}
		item := FileSetEntry{Path: slashRel, SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
		if utf8.Valid(data) {
			item.Encoding, item.Content = "utf8", string(data)
		} else {
			item.Encoding, item.Content = "base64", base64.StdEncoding.EncodeToString(data)
		}
		set.Files = append(set.Files, item)
		return nil
	})
	if err != nil {
		return FileSet{}, err
	}
	sort.Slice(set.Files, func(i, j int) bool { return set.Files[i].Path < set.Files[j].Path })
	return set, nil
}

// UnpackFileSet writes a file set below root after checking every path and
// digest. Paths must be relative, slash-separated and stay inside root.
func UnpackFileSet(data []byte, root string) ([]string, error) {
	var set FileSet
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&set); err != nil {
		return nil, fmt.Errorf("decode file set: %w", err)
	}
	seen := map[string]bool{}
	var written []string
	for _, item := range set.Files {
		clean, err := cleanFileSetPath(item.Path)
		if err != nil {
			return nil, err
		}
		if seen[clean] {
			return nil, fmt.Errorf("file set: duplicate path %q", item.Path)
		}
		seen[clean] = true
		var content []byte
		switch item.Encoding {
		case "utf8", "":
			content = []byte(item.Content)
		case "base64":
			content, err = base64.StdEncoding.DecodeString(item.Content)
			if err != nil {
				return nil, fmt.Errorf("file set: %s: %w", item.Path, err)
			}
		default:
			return nil, fmt.Errorf("file set: %s: unknown encoding %q", item.Path, item.Encoding)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(content)); !strings.EqualFold(got, item.SHA256) {
			return nil, fmt.Errorf("file set: %s: sha256 is %s, entry says %s", item.Path, got, item.SHA256)
		}
		target := filepath.Join(root, filepath.FromSlash(clean))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(target, content, 0o644); err != nil {
			return nil, err
		}
		written = append(written, clean)
	}
	return written, nil
}

func cleanFileSetPath(value string) (string, error) {
	if value == "" || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") || strings.ContainsRune(value, 0) {
		return "", fmt.Errorf("file set: invalid path %q", value)
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != value {
		return "", fmt.Errorf("file set: invalid path %q", value)
	}
	return clean, nil
}

// ExtensionPluginIO lets a ProcessorImplementation wrap an existing Kalo
// plugin (old ABI: /input, /output, config as argv JSON) without plugin code.
const ExtensionPluginIO = "KALO:PLUGIN-IO"

const PluginABIv1 = "kalo-plugin-v1"

type PluginIO struct {
	ABI    string         `json:"abi"`
	Input  PluginIOInput  `json:"input"`
	Output PluginIOOutput `json:"output"`
	Config map[string]any `json:"config,omitempty"`
}

type PluginIOInput struct {
	// Filename is the name the input value gets inside /input. Ignored when
	// the input binding is a file set, which is unpacked into /input instead.
	Filename string `json:"filename,omitempty"`
}

type PluginIOOutput struct {
	// Pack is "single" (default: the plugin writes exactly one file) or
	// "fileset" (everything below /output is packed into one file-set value).
	Pack string `json:"pack,omitempty"`
}

// PluginIOFor returns the KALO:PLUGIN-IO extension of an implementation, or
// nil when it has none.
func PluginIOFor(implementation *Implementation) (*PluginIO, error) {
	raw, ok := implementation.Extensions[ExtensionPluginIO]
	if !ok {
		return nil, nil
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var io PluginIO
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&io); err != nil {
		return nil, fmt.Errorf("implementation %s: %s: %w", implementation.ImplementationID, ExtensionPluginIO, err)
	}
	if io.ABI != PluginABIv1 {
		return nil, fmt.Errorf("implementation %s: %s: abi must be %q, got %q", implementation.ImplementationID, ExtensionPluginIO, PluginABIv1, io.ABI)
	}
	if io.Input.Filename != "" {
		if _, err := cleanFileSetPath(io.Input.Filename); err != nil || strings.Contains(io.Input.Filename, "/") {
			return nil, fmt.Errorf("implementation %s: %s: input.filename must be a plain file name, got %q", implementation.ImplementationID, ExtensionPluginIO, io.Input.Filename)
		}
	}
	switch io.Output.Pack {
	case "":
		io.Output.Pack = "single"
	case "single", "fileset":
	default:
		return nil, fmt.Errorf("implementation %s: %s: output.pack must be single or fileset, got %q", implementation.ImplementationID, ExtensionPluginIO, io.Output.Pack)
	}
	return &io, nil
}

func mediaTypeOrJSON(mediaType string) string {
	if mediaType == "" {
		return "application/json"
	}
	return mediaType
}
