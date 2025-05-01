package registry

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// PluginIdentifier uniquely identifies a plugin in the registry
type PluginIdentifier string

// PluginVersion represents a semver version of a plugin
type PluginVersion string

// PluginMetadata contains information about a specific plugin
// type PluginMetadata struct {
// 	// Plugin identifier in the format @org/name
// 	ID PluginIdentifier `json:"id"`

// 	// Version of the plugin
// 	Version PluginVersion `json:"version"`

// 	// Description of what the plugin does
// 	Description string `json:"description,omitempty"`

// 	// Author of the plugin
// 	Author string `json:"author,omitempty"`

// 	// License of the plugin
// 	License string `json:"license,omitempty"`

// 	// Homepage URL for the plugin
// 	Homepage string `json:"homepage,omitempty"`

// 	// Repository URL for the plugin source code
// 	Repository string `json:"repository,omitempty"`

// 	// Tags associated with the plugin
// 	Tags []string `json:"tags,omitempty"`

// 	// InputSpec that this plugin consumes
// 	InputSpec string `json:"inputSpec"`

// 	// OutputSpec that this plugin produces
// 	OutputSpec string `json:"outputSpec"`

// 	// Dependencies on other plugins or modules
// 	Dependencies map[PluginIdentifier]PluginVersion `json:"dependencies,omitempty"`

// 	// Size of the plugin WASM binary in bytes
// 	Size int64 `json:"size"`

// 	// SHA256 hash of the plugin binary
// 	SHA256 string `json:"sha256"`

// 	// PublishedAt timestamp when the plugin was published
// 	PublishedAt time.Time `json:"publishedAt"`
// }

// PluginLockInfo represents a plugin entry in the lockfile
type PluginLockInfo struct {
	// Version of the plugin
	Version PluginVersion `yaml:"version"`

	// ResolvedHash of the downloaded WASM binary
	ResolvedHash string `yaml:"resolvedHash"`

	// Location where the plugin is stored locally
	Location string `yaml:"location"`

	// DownloadedAt timestamp when the plugin was downloaded
	DownloadedAt time.Time `yaml:"downloadedAt"`
}

// LockFile represents the kalo.lock file structure
type LockFile struct {
	// GeneratedAt timestamp when the lockfile was generated
	GeneratedAt time.Time `yaml:"generatedAt"`

	// Plugins indexed by their identifier
	Plugins map[PluginIdentifier]PluginLockInfo `yaml:"plugins"`
}

// // Registry defines the interface for interacting with the Kalo plugin registry
// type Registry interface {
// 	// GetPluginMetadata retrieves metadata for a plugin
// 	GetPluginMetadata(id PluginIdentifier, version PluginVersion) (*PluginMetadata, error)

// 	// SearchPlugins searches for plugins matching criteria
// 	SearchPlugins(query string, tags []string) ([]PluginMetadata, error)

// 	// DownloadPlugin downloads a plugin to the local cache
// 	DownloadPlugin(id PluginIdentifier, version PluginVersion) (string, error)

// 	// ResolveVersion resolves a version constraint to a specific version
// 	ResolveVersion(id PluginIdentifier, versionConstraint string) (PluginVersion, error)

// 	// ValidatePluginHash validates the SHA256 hash of a plugin
// 	ValidatePluginHash(id PluginIdentifier, version PluginVersion, filePath string) (bool, error)

// 	// GenerateLockFile generates a lockfile from the current state
// 	GenerateLockFile(configPath string, pluginVersions map[PluginIdentifier]PluginVersion) (*LockFile, error)
// }

// CalculateSHA256 calculates the SHA256 hash of a file
func CalculateSHA256(filePath string) (string, error) {
	f, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file for hashing: %w", err)
	}

	hash := sha256.Sum256(f)
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}
