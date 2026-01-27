package registry

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultRegistryURL is the default URL for the Kalo registry
	DefaultRegistryURL = "https://registry.kalo.build"

	// DefaultCacheDir is the default directory for storing downloaded plugins
	DefaultCacheDir = ".kalo/plugins"
)

// RegistryClientOptions contains options for configuring the registry client
type RegistryClientOptions struct {
	// RegistryURL is the base URL for the registry API
	RegistryURL string

	// CacheDir is the directory where downloaded plugins are stored
	CacheDir string

	// HttpClient is the HTTP client to use for requests
	HttpClient *http.Client

	// OfflineMode determines if the client operates in offline mode
	OfflineMode bool
}

// RegistryClient is a client for interacting with the Kalo plugin registry
type RegistryClient struct {
	options    RegistryClientOptions
	httpClient *http.Client
}

// NewRegistryClient creates a new registry client with the given options
func NewRegistryClient(options *RegistryClientOptions) *RegistryClient {
	opts := RegistryClientOptions{
		RegistryURL: DefaultRegistryURL,
		CacheDir:    DefaultCacheDir,
		HttpClient:  http.DefaultClient,
		OfflineMode: false,
	}

	if options != nil {
		if options.RegistryURL != "" {
			opts.RegistryURL = options.RegistryURL
		}

		if options.CacheDir != "" {
			opts.CacheDir = options.CacheDir
		}

		if options.HttpClient != nil {
			opts.HttpClient = options.HttpClient
		}

		opts.OfflineMode = options.OfflineMode
	}

	return &RegistryClient{
		options:    opts,
		httpClient: opts.HttpClient,
	}
}

// GetPluginMetadata retrieves metadata for a plugin
func (c *RegistryClient) GetPluginMetadata(id PluginIdentifier, version PluginVersion) (*PluginMetadata, error) {
	if c.options.OfflineMode {
		return nil, fmt.Errorf("cannot fetch plugin metadata in offline mode")
	}

	url := fmt.Sprintf("%s/v1/plugins/%s/versions/%s", c.options.RegistryURL, id, version)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch plugin metadata: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin metadata: %w", err)
	}

	var metadata PluginMetadata
	err = yaml.Unmarshal(body, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal plugin metadata: %w", err)
	}

	return &metadata, nil
}

// SearchPlugins searches for plugins matching criteria
func (c *RegistryClient) SearchPlugins(query string, tags []string) ([]PluginMetadata, error) {
	if c.options.OfflineMode {
		return nil, fmt.Errorf("cannot search plugins in offline mode")
	}

	// Make an HTTP request to the registry API
	url := fmt.Sprintf("%s/v1/plugins?query=%s&tags=%s", c.options.RegistryURL, query, strings.Join(tags, ","))
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to search plugins: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin metadata: %w", err)
	}

	var metadata []PluginMetadata
	err = yaml.Unmarshal(body, &metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal plugin metadata: %w", err)
	}

	return metadata, nil
}

// DownloadPlugin downloads a plugin to the local cache
func (c *RegistryClient) DownloadPlugin(id PluginIdentifier, version PluginVersion) (string, error) {
	if c.options.OfflineMode {
		return "", fmt.Errorf("cannot download plugins in offline mode")
	}

	// Get plugin metadata first to verify hash later
	metadata, err := c.GetPluginMetadata(id, version)
	if err != nil {
		return "", fmt.Errorf("failed to get plugin metadata: %w", err)
	}

	// Ensure cache directory exists
	if err := os.MkdirAll(c.options.CacheDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Generate filename based on plugin ID and version
	filename := c.getPluginFilename(id, version)
	localPath := filepath.Join(c.options.CacheDir, filename)

	// Check if plugin is already downloaded and verified
	if _, err := os.Stat(localPath); err == nil {
		// Verify hash of existing file
		hash, err := CalculateSHA256(localPath)
		if err == nil && hash == metadata.SHA256 {
			return localPath, nil
		}
		// If hash verification fails, re-download
		_ = os.Remove(localPath)
	}

	url := fmt.Sprintf("%s/v1/plugins/%s/versions/%s/download", c.options.RegistryURL, id, version)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to download plugin: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to download plugin: HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Create a temporary file first
	tmpFile := localPath + ".tmp"
	f, err := os.Create(tmpFile)
	if err != nil {
		return "", fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer func() {
		f.Close()
		if err != nil {
			os.Remove(tmpFile)
		}
	}()

	// Copy to temporary file
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to write plugin data: %w", err)
	}

	// Sync to ensure all data is written
	if err := f.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync file: %w", err)
	}

	// Close the file before calculating hash
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("failed to close file: %w", err)
	}

	// Verify hash
	hash, err := CalculateSHA256(tmpFile)
	if err != nil {
		return "", fmt.Errorf("failed to calculate hash: %w", err)
	}

	if hash != metadata.SHA256 {
		return "", fmt.Errorf("hash mismatch: expected %s, got %s", metadata.SHA256, hash)
	}

	// Move temporary file to final location
	if err := os.Rename(tmpFile, localPath); err != nil {
		return "", fmt.Errorf("failed to move file to final location: %w", err)
	}

	return localPath, nil
}

// ResolveVersion resolves a version constraint to a specific version
func (c *RegistryClient) ResolveVersion(id PluginIdentifier, versionConstraint string) (PluginVersion, error) {
	if c.options.OfflineMode {
		return "", fmt.Errorf("cannot resolve version in offline mode")
	}

	// Fetch available versions from the registry and find the best match for the version constraint
	versions, err := c.SearchPlugins(string(id), []string{})
	if err != nil {
		return "", fmt.Errorf("failed to search plugins: %w", err)
	}

	// Find the best match for the constraint
	var bestMatch PluginVersion
	for _, version := range versions {
		if c.versionSatisfiesConstraint(version.Version, versionConstraint) && c.versionIsNewer(version.Version, bestMatch) {
			bestMatch = version.Version
		}
	}

	if bestMatch == "" {
		return "", fmt.Errorf("no version satisfies the constraint: %s", versionConstraint)
	}

	return bestMatch, nil
}

// versionSatisfiesConstraint checks if a version satisfies a version constraint
func (c *RegistryClient) versionSatisfiesConstraint(version PluginVersion, constraint string) bool {
	// Strip 'v' prefix if present for semver compatibility
	versionStr := strings.TrimPrefix(string(version), "v")

	// Parse the version
	v, err := semver.NewVersion(versionStr)
	if err != nil {
		// Log instead of returning error to maintain the method signature
		log.Printf("Warning: Invalid semver format '%s': %v", version, err)
		return false
	}

	// Parse the constraint
	constraints, err := semver.NewConstraint(constraint)
	if err != nil {
		log.Printf("Warning: Invalid constraint format '%s': %v", constraint, err)
		return false
	}

	// Check if the version satisfies the constraint
	return constraints.Check(v)
}

// versionIsNewer checks if version1 is newer than version2
func (c *RegistryClient) versionIsNewer(version1 PluginVersion, version2 PluginVersion) bool {
	if version2 == "" {
		return true
	}

	v1Str := strings.TrimPrefix(string(version1), "v")
	v2Str := strings.TrimPrefix(string(version2), "v")

	// Parse both versions
	v1, err := semver.NewVersion(v1Str)
	if err != nil {
		log.Printf("Warning: Invalid semver format '%s': %v", version1, err)
		return version1 > version2
	}

	v2, err := semver.NewVersion(v2Str)
	if err != nil {
		log.Printf("Warning: Invalid semver format '%s': %v", version2, err)
		return version1 > version2
	}

	return v1.GreaterThan(v2)
}

// ValidatePluginHash validates the SHA256 hash of a plugin
func (c *RegistryClient) ValidatePluginHash(id PluginIdentifier, version PluginVersion, filePath string) (bool, error) {
	// Get the expected hash from the registry
	metadata, err := c.GetPluginMetadata(id, version)
	if err != nil {
		return false, fmt.Errorf("failed to get plugin metadata: %w", err)
	}

	// Calculate the hash of the downloaded file
	actualHash, err := CalculateSHA256(filePath)
	if err != nil {
		return false, fmt.Errorf("failed to calculate file hash: %w", err)
	}

	// Compare the hashes
	return metadata.SHA256 == actualHash, nil
}

// GenerateLockFile generates a lockfile from the current state
func (c *RegistryClient) GenerateLockFile(configPath string, pluginVersions map[PluginIdentifier]PluginVersion) (*LockFile, error) {
	lockFile := &LockFile{
		GeneratedAt: time.Now(),
		Plugins:     make(map[PluginIdentifier]PluginLockInfo),
	}

	for id, version := range pluginVersions {
		// Download the plugin if it's not already downloaded
		localPath, err := c.DownloadPlugin(id, version)
		if err != nil {
			return nil, fmt.Errorf("failed to download plugin %s@%s: %w", id, version, err)
		}

		// Calculate the hash of the downloaded plugin
		hash, err := CalculateSHA256(localPath)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate hash for plugin %s: %w", id, err)
		}

		// Add the plugin to the lockfile
		lockFile.Plugins[id] = PluginLockInfo{
			Version:      version,
			ResolvedHash: hash,
			Location:     localPath,
			DownloadedAt: time.Now(),
		}
	}

	return lockFile, nil
}

// SaveLockFile saves the lockfile to disk
func (c *RegistryClient) SaveLockFile(lockFile *LockFile, filePath string) error {
	data, err := yaml.Marshal(lockFile)
	if err != nil {
		return fmt.Errorf("failed to marshal lockfile: %w", err)
	}

	// Add a header comment
	content := "# kalo.lock - generated from kalo.yaml\n" + string(data)

	err = os.WriteFile(filePath, []byte(content), 0644)
	if err != nil {
		return fmt.Errorf("failed to write lockfile: %w", err)
	}

	return nil
}

// LoadLockFile loads a lockfile from disk
func (c *RegistryClient) LoadLockFile(filePath string) (*LockFile, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read lockfile: %w", err)
	}

	lockFile := &LockFile{}
	err = yaml.Unmarshal(data, lockFile)
	if err != nil {
		return nil, fmt.Errorf("failed to parse lockfile: %w", err)
	}

	return lockFile, nil
}

// getPluginFilename generates a filename for a plugin based on its ID and version
func (c *RegistryClient) getPluginFilename(id PluginIdentifier, version PluginVersion) string {
	// Convert the ID to a valid filename (replace / with - and remove @)
	name := strings.ReplaceAll(string(id), "/", "-")
	name = strings.TrimPrefix(name, "@")

	return fmt.Sprintf("%s-%s.wasm", name, version)
}
