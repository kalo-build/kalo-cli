package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/kalo-build/kalo-cli/pkg/hostfuncs"
	"github.com/kalo-build/kalo-cli/pkg/registry"
	"github.com/spf13/cobra"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"gopkg.in/yaml.v3"
)

// KaloConfig represents the structure of kalo.yaml
type KaloConfig struct {
	Stores    map[string]Store            `yaml:"stores"`
	Config    map[string]interface{}      `yaml:"config"`
	Pipelines map[string]Pipeline         `yaml:"pipelines"`
	Plugins   map[string]PluginDefinition `yaml:"plugins"`
}

// StoreType constants
const (
	StoreTypeLocalFileSystem  = "localFileSystem"
	StoreTypeGitRepository    = "gitRepository"
	StoreTypeCloudSqlDatabase = "cloudSqlDatabase"
)

// Store represents a data store configuration.
// The Type field determines which options are relevant.
type Store struct {
	Format  string         `yaml:"format"`
	Type    string         `yaml:"type"` // localFileSystem, gitRepository, cloudSqlDatabase
	Options map[string]any `yaml:"options,omitempty"`
}

// GetStringOption returns a string option value.
func (s Store) GetStringOption(key string, defaultVal string) string {
	if s.Options == nil {
		return defaultVal
	}
	if v, ok := s.Options[key]; ok {
		if str, ok := v.(string); ok {
			return str
		}
	}
	return defaultVal
}

// Path returns the path option for localFileSystem stores.
func (s Store) Path() string {
	return s.GetStringOption("path", "")
}

// Connection returns the connection option for cloudSqlDatabase stores.
func (s Store) Connection() string {
	return s.GetStringOption("connection", "")
}

// GitRepoRoot returns the repoRoot option for gitRepository stores.
func (s Store) GitRepoRoot() string {
	return s.GetStringOption("repoRoot", ".")
}

// GitRef returns the ref option for gitRepository stores.
func (s Store) GitRef() string {
	return s.GetStringOption("ref", "HEAD")
}

// GitSubPath returns the subPath option for gitRepository stores.
func (s Store) GitSubPath() string {
	return s.GetStringOption("subPath", "")
}

// Pipeline represents a pipeline configuration
type Pipeline struct {
	Description string  `yaml:"description,omitempty"` // Short description shown in 'kalo list'
	Alias       string  `yaml:"alias,omitempty"`       // Optional short alias for the pipeline (e.g., "up" for "migrate-up")
	Stages      []Stage `yaml:"stages"`
}

// Stage represents a stage in a pipeline
type Stage struct {
	Name   string                 `yaml:"name"`
	Steps  []string               `yaml:"steps"`
	Config map[string]interface{} `yaml:"config,omitempty"` // Per-stage config, merged with global plugin config
}

// PluginDefinition represents a plugin configuration
type PluginDefinition struct {
	Version string                  `yaml:"version"`
	Input   *PluginIOSpec           `yaml:"input,omitempty"`
	Inputs  map[string]PluginIOSpec `yaml:"inputs,omitempty"` // Multiple named inputs
	Output  *PluginIOSpec           `yaml:"output,omitempty"`
	Config  map[string]any          `yaml:"config,omitempty"`
}

// PluginIOSpec represents a plugin's input or output specification
type PluginIOSpec struct {
	Format string `yaml:"format"`
	Store  string `yaml:"store"`
}

const (
	KaloConfigFile     = "kalo.yaml"
	KaloLockFile       = "kalo.lock"
	MaxFileSize        = 200 * 1024 // 200 KB
	DefaultRegistryURL = "https://registry.kalo.build"
	DefaultPluginCache = ".kalo/plugins"
)

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: No .env file found. Using system environment variables.")
	}

	rootCmd := &cobra.Command{
		Use:     "kalo",
		Short:   "Kalo CLI is a tool for managing Kalo projects",
		Long:    `Kalo CLI helps you manage Kalo projects, run plugins, and more.`,
		Version: "0.0.1",
	}

	rootCmd.AddCommand(compileCommand())
	rootCmd.AddCommand(runCommand())
	rootCmd.AddCommand(pluginCommand())
	rootCmd.AddCommand(listCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func compileCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compile",
		Short: "Run the default compile pipeline",
		Long:  `Run the default compile pipeline defined in kalo.yaml`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := runTarget("compile"); err != nil {
				log.Fatalf("Compilation failed: %v", err)
			}
			log.Println("Compilation completed successfully")
		},
	}

	return cmd
}

func runCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run <pipeline-or-plugin>",
		Short: "Run a pipeline or individual plugin",
		Long: `Run a named pipeline or a single plugin.

Examples:
  kalo run compile                              # Run the 'compile' pipeline
  kalo run morphe-diff-and-migrate              # Run the 'morphe-diff-and-migrate' pipeline
  kalo run @kalo-build/plugin-morphe-db-manager # Run a single plugin`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := runTarget(args[0]); err != nil {
				log.Fatalf("Run failed: %v", err)
			}
			log.Println("Run completed successfully")
		},
	}

	return cmd
}

func listCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available pipelines and plugins",
		Long:  `List all pipelines defined in kalo.yaml, showing names and aliases.`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := listPipelines(); err != nil {
				log.Fatalf("List failed: %v", err)
			}
		},
	}

	return cmd
}

// listPipelines reads kalo.yaml and displays all available pipelines
func listPipelines() error {
	// Read kalo.yaml
	configData, err := os.ReadFile("kalo.yaml")
	if err != nil {
		return fmt.Errorf("failed to read kalo.yaml: %w", err)
	}

	var config KaloConfig
	if err := yaml.Unmarshal(configData, &config); err != nil {
		return fmt.Errorf("failed to parse kalo.yaml: %w", err)
	}

	fmt.Println("Available pipelines:")
	fmt.Println()

	// Collect and sort pipeline names for consistent output
	names := make([]string, 0, len(config.Pipelines))
	for name := range config.Pipelines {
		names = append(names, name)
	}
	// Sort alphabetically
	for i := 0; i < len(names)-1; i++ {
		for j := i + 1; j < len(names); j++ {
			if names[i] > names[j] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}

	for _, name := range names {
		pipeline := config.Pipelines[name]

		// Build the name/alias part
		nameStr := name
		if pipeline.Alias != "" {
			nameStr = fmt.Sprintf("%s (%s)", name, pipeline.Alias)
		}

		// Print with description if available
		if pipeline.Description != "" {
			fmt.Printf("  %-30s %s\n", nameStr, pipeline.Description)
		} else {
			fmt.Printf("  %s\n", nameStr)
		}
	}

	fmt.Println()
	fmt.Println("Run with: kalo run <pipeline-name-or-alias>")

	return nil
}

// runTarget runs a pipeline or individual plugin by name.
func runTarget(target string) error {
	config, err := readKaloConfig()
	if err != nil {
		return fmt.Errorf("failed to read kalo.yaml: %w", err)
	}

	lockFile, err := readKaloLock()
	if err != nil {
		return fmt.Errorf("failed to read kalo.lock: %w", err)
	}

	// Ensure all localFileSystem store directories exist
	for name, store := range config.Stores {
		if store.Type != StoreTypeLocalFileSystem {
			continue
		}
		storePath := store.Path()
		if storePath == "" {
			return fmt.Errorf("store %s: localFileSystem requires 'path' option", name)
		}
		storePath = os.ExpandEnv(storePath)
		log.Printf("Creating store directory for %s: %s", name, storePath)
		if err := os.MkdirAll(storePath, 0755); err != nil {
			return fmt.Errorf("failed to create store directory %s: %w", storePath, err)
		}
	}

	ctx := context.Background()
	wasmRuntime := wazero.NewRuntime(ctx)
	defer wasmRuntime.Close(ctx)

	wasi_snapshot_preview1.MustInstantiate(ctx, wasmRuntime)

	// Create shared host functions and register once
	kaloHost := hostfuncs.NewKaloHost()
	defer kaloHost.Close()

	if err := kaloHost.Register(ctx, wasmRuntime); err != nil {
		return fmt.Errorf("failed to register host functions: %w", err)
	}

	// Check if target is a plugin (starts with @) or a pipeline
	if strings.HasPrefix(target, "@") {
		return runSinglePlugin(ctx, wasmRuntime, kaloHost, target, config, lockFile)
	}
	return runPipeline(ctx, wasmRuntime, kaloHost, target, config, lockFile)
}

// runSinglePlugin runs a single plugin by name.
func runSinglePlugin(ctx context.Context, wasmRuntime wazero.Runtime, kaloHost *hostfuncs.KaloHost, pluginName string, config *KaloConfig, lockFile *registry.LockFile) error {
	log.Printf("Running plugin: %s", pluginName)

	pluginDef, exists := config.Plugins[pluginName]
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.yaml", pluginName)
	}

	pluginLock, exists := lockFile.Plugins[registry.PluginIdentifier(pluginName)]
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.lock", pluginName)
	}

	// Check if plugin file exists, if not attempt to download
	pluginPath := pluginLock.Location
	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		log.Printf("Plugin %s not found at %s, attempting download...", pluginName, pluginPath)
		downloadedPath, downloadErr := downloadPluginFromRegistry(pluginName, string(pluginDef.Version))
		if downloadErr != nil {
			return fmt.Errorf("plugin file not found and download failed: %w", downloadErr)
		}
		pluginPath = downloadedPath
	}

	// Merge plugin-specific config from config section
	if pluginConfig, ok := config.Config[pluginName]; ok {
		if configMap, ok := pluginConfig.(map[string]any); ok {
			if pluginDef.Config == nil {
				pluginDef.Config = configMap
			} else {
				for k, v := range configMap {
					pluginDef.Config[k] = v
				}
			}
		}
	}

	return executePlugin(ctx, wasmRuntime, kaloHost, pluginPath, config.Stores, pluginDef)
}

// resolvePipeline looks up a pipeline by name or alias.
// Returns (resolvedName, pipeline, found).
func resolvePipeline(nameOrAlias string, pipelines map[string]Pipeline) (string, Pipeline, bool) {
	// First, try direct name lookup
	if pipeline, exists := pipelines[nameOrAlias]; exists {
		return nameOrAlias, pipeline, true
	}

	// Then, try to find by alias
	for name, pipeline := range pipelines {
		if pipeline.Alias == nameOrAlias {
			return name, pipeline, true
		}
	}

	return "", Pipeline{}, false
}

// runPipeline runs all steps in a pipeline.
func runPipeline(ctx context.Context, wasmRuntime wazero.Runtime, kaloHost *hostfuncs.KaloHost, pipelineName string, config *KaloConfig, lockFile *registry.LockFile) error {
	// Resolve pipeline name (check direct name first, then aliases)
	resolvedName, pipeline, exists := resolvePipeline(pipelineName, config.Pipelines)
	if !exists {
		return fmt.Errorf("pipeline '%s' not found in kalo.yaml (checked name and aliases)", pipelineName)
	}

	if resolvedName != pipelineName {
		log.Printf("Running pipeline: %s (alias for %s)", pipelineName, resolvedName)
	} else {
		log.Printf("Running pipeline: %s", pipelineName)
	}

	for _, stage := range pipeline.Stages {
		log.Printf("Running stage: %s", stage.Name)

		for _, step := range stage.Steps {
			if !strings.HasPrefix(step, "plugin: ") {
				return fmt.Errorf("invalid step format: %s (expected 'plugin: @org/name')", step)
			}
			pluginName := strings.TrimPrefix(step, "plugin: ")

			if err := runPluginWithStageConfig(ctx, wasmRuntime, kaloHost, pluginName, stage.Config, config, lockFile); err != nil {
				return fmt.Errorf("plugin %s failed: %w", pluginName, err)
			}
		}
	}

	return nil
}

// runPluginWithStageConfig runs a plugin with per-stage config merged with global config.
func runPluginWithStageConfig(ctx context.Context, wasmRuntime wazero.Runtime, kaloHost *hostfuncs.KaloHost, pluginName string, stageConfig map[string]interface{}, config *KaloConfig, lockFile *registry.LockFile) error {
	log.Printf("Running plugin: %s", pluginName)

	pluginDef, exists := config.Plugins[pluginName]
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.yaml", pluginName)
	}

	pluginLock, exists := lockFile.Plugins[registry.PluginIdentifier(pluginName)]
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.lock", pluginName)
	}

	// Check if plugin file exists, if not attempt to download
	pluginPath := pluginLock.Location
	if _, err := os.Stat(pluginPath); os.IsNotExist(err) {
		log.Printf("Plugin %s not found at %s, attempting download...", pluginName, pluginPath)
		downloadedPath, downloadErr := downloadPluginFromRegistry(pluginName, string(pluginDef.Version))
		if downloadErr != nil {
			return fmt.Errorf("plugin file not found and download failed: %w", downloadErr)
		}
		pluginPath = downloadedPath
	}

	// Start with a copy of the plugin definition's config
	mergedConfig := make(map[string]interface{})

	// 1. First, merge plugin-specific config from global config section
	if pluginConfig, ok := config.Config[pluginName]; ok {
		if configMap, ok := pluginConfig.(map[string]any); ok {
			for k, v := range configMap {
				mergedConfig[k] = v
			}
		}
	}

	// 2. Then, merge plugin definition's config
	if pluginDef.Config != nil {
		for k, v := range pluginDef.Config {
			mergedConfig[k] = v
		}
	}

	// 3. Finally, merge stage-specific config (highest priority)
	if stageConfig != nil {
		for k, v := range stageConfig {
			mergedConfig[k] = v
		}
	}

	// Create a copy of pluginDef with merged config
	pluginDefWithConfig := pluginDef
	pluginDefWithConfig.Config = mergedConfig

	return executePlugin(ctx, wasmRuntime, kaloHost, pluginPath, config.Stores, pluginDefWithConfig)
}

func pluginCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Manage Kalo plugins",
		Long:  `Commands for managing Kalo plugins including installation, removal, and listing.`,
	}

	cmd.AddCommand(pluginInstallCommand())

	return cmd
}

func pluginInstallCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install [plugin-id[@version]]",
		Short: "Install a Kalo plugin",
		Long: `Install a Kalo plugin into the current project.
The plugin will be downloaded and added to the kalo.yaml manifest.
If no version is specified, the latest version will be used.

Example:
  kalo plugin install @kalo-build/plugin-morphe-psql-types@v1.0.0`,
		Args: cobra.ExactArgs(1),
		RunE: runPluginInstall,
	}

	return cmd
}

func runPluginInstall(cmd *cobra.Command, args []string) error {
	// Parse plugin ID and version
	pluginID, version, err := parsePluginArg(args[0])
	if err != nil {
		return fmt.Errorf("invalid plugin identifier: %w", err)
	}

	// Create registry client (registry URL can be overridden via KALO_REGISTRY_URL env var)
	registryURL := os.Getenv("KALO_REGISTRY_URL")
	if registryURL == "" {
		registryURL = DefaultRegistryURL
	}

	client := registry.NewRegistryClient(&registry.RegistryClientOptions{
		RegistryURL: registryURL,
		CacheDir:    DefaultPluginCache,
		OfflineMode: false,
	})

	// If no version specified, get latest
	if version == "" {
		metadata, err := client.SearchPlugins(string(pluginID), nil)
		if err != nil {
			return fmt.Errorf("failed to search for plugin: %w", err)
		}
		if len(metadata) == 0 {
			return fmt.Errorf("plugin %s not found", pluginID)
		}
		// Use the latest version
		version = metadata[0].Version
	}

	// Get plugin metadata to verify it exists
	metadata, err := client.GetPluginMetadata(pluginID, version)
	if err != nil {
		return fmt.Errorf("failed to get plugin metadata: %w", err)
	}

	// Try to get plugin manifest for better configuration
	manifest, err := client.GetPluginManifest(pluginID, version)
	if err != nil {
		log.Printf("Warning: Could not fetch plugin manifest: %v", err)
		// Continue without manifest
	}

	// Load existing kalo.yaml or create a new one
	config, err := readKaloConfig()
	if err != nil {
		// Create a new config if kalo.yaml doesn't exist
		if os.IsNotExist(err) || strings.Contains(err.Error(), "cannot find the file") {
			fmt.Println("Creating new kalo.yaml...")
			config = &KaloConfig{
				Stores:    make(map[string]Store),
				Config:    make(map[string]interface{}),
				Pipelines: make(map[string]Pipeline),
				Plugins:   make(map[string]PluginDefinition),
			}
		} else {
			return fmt.Errorf("failed to load kalo.yaml: %w", err)
		}
	}

	// Check if plugin is already installed with same version
	if existingPlugin, exists := config.Plugins[string(pluginID)]; exists {
		if existingPlugin.Version == string(version) {
			fmt.Printf("Plugin %s@%s is already installed.\n", pluginID, version)
			fmt.Println("Nothing to do.")
			return nil
		}
		fmt.Printf("Updating %s from %s to %s...\n", pluginID, existingPlugin.Version, version)
	}

	// Configure stores and plugin definition based on manifest or fallback to metadata
	var pluginDef PluginDefinition
	if manifest != nil {
		pluginDef, err = configureFromManifest(config, manifest, string(version))
		if err != nil {
			return fmt.Errorf("failed to configure from manifest: %w", err)
		}
	} else {
		pluginDef, err = configureFromMetadata(config, metadata, string(version))
		if err != nil {
			return fmt.Errorf("failed to configure from metadata: %w", err)
		}
	}

	// Add plugin to config
	if config.Plugins == nil {
		config.Plugins = make(map[string]PluginDefinition)
	}
	config.Plugins[string(pluginID)] = pluginDef

	// Add config section for the plugin with defaults from manifest
	if config.Config == nil {
		config.Config = make(map[string]interface{})
	}
	if _, exists := config.Config[string(pluginID)]; !exists {
		pluginConfig := make(map[string]interface{})
		// Populate with default values from manifest's configSchema
		if manifest != nil && manifest.ConfigSchema != nil {
			for key, option := range manifest.ConfigSchema {
				if option.Default != nil {
					pluginConfig[key] = option.Default
				}
			}
		}
		config.Config[string(pluginID)] = pluginConfig
	}

	// Create a default pipeline if none exist
	if config.Pipelines == nil {
		config.Pipelines = make(map[string]Pipeline)
	}
	if len(config.Pipelines) == 0 {
		pipelineName := "compile"
		if manifest != nil && manifest.Modes != nil {
			// For plugins with modes, use the default mode name
			for name, mode := range manifest.Modes {
				if mode.IsDefault {
					pipelineName = name
					break
				}
			}
		}

		config.Pipelines[pipelineName] = Pipeline{
			Description: fmt.Sprintf("Run %s", pluginID),
			Stages: []Stage{
				{
					Name:  pipelineName,
					Steps: []string{fmt.Sprintf("plugin: %s", pluginID)},
				},
			},
		}
		fmt.Printf("Created pipeline: %s\n", pipelineName)
	}

	// Save kalo.yaml
	err = saveKaloConfig(KaloConfigFile, config)
	if err != nil {
		return fmt.Errorf("failed to save kalo.yaml: %w", err)
	}

	// Download plugin
	localPath, err := client.DownloadPlugin(pluginID, version)
	if err != nil {
		return fmt.Errorf("failed to download plugin: %w", err)
	}

	// Generate/update lockfile
	pluginVersions := make(map[registry.PluginIdentifier]registry.PluginVersion)
	for id, plugin := range config.Plugins {
		pluginVersions[registry.PluginIdentifier(id)] = registry.PluginVersion(plugin.Version)
	}

	lockFile, err := client.GenerateLockFile(KaloConfigFile, pluginVersions)
	if err != nil {
		return fmt.Errorf("failed to generate lockfile: %w", err)
	}

	err = client.SaveLockFile(lockFile, KaloLockFile)
	if err != nil {
		return fmt.Errorf("failed to save lockfile: %w", err)
	}

	fmt.Printf("Successfully installed %s@%s to %s\n", pluginID, version, localPath)
	return nil
}

// configureFromManifest creates stores and plugin definition based on manifest
func configureFromManifest(config *KaloConfig, manifest *registry.PluginManifest, version string) (PluginDefinition, error) {
	pluginDef := PluginDefinition{
		Version: version,
	}

	// Helper to add a store from suggested store config
	addStore := func(spec *registry.ManifestIOSpec) (string, error) {
		if spec == nil {
			return "", nil
		}

		storeName := spec.SuggestedStore.Name
		if storeName == "" {
			// Generate store name from format
			storeName = formatToStoreName(spec.Format)
		}

		// Add store if it doesn't exist
		if _, exists := config.Stores[storeName]; !exists {
			store := Store{
				Format:  spec.Format,
				Type:    spec.SuggestedStore.Type,
				Options: make(map[string]any),
			}

			// Set default type if not specified
			if store.Type == "" {
				store.Type = StoreTypeLocalFileSystem
			}

			// Configure based on store type
			switch store.Type {
			case StoreTypeLocalFileSystem:
				path := spec.SuggestedStore.Path
				if path == "" {
					path = "./" + strings.ToLower(storeName)
				}
				store.Options["path"] = path
			case StoreTypeGitRepository:
				store.Options["repoRoot"] = spec.SuggestedStore.RepoRoot
				store.Options["ref"] = spec.SuggestedStore.Ref
				store.Options["subPath"] = spec.SuggestedStore.SubPath
			case StoreTypeCloudSqlDatabase:
				conn := spec.SuggestedStore.Connection
				if conn == "" {
					conn = "$DATABASE_URL"
				}
				store.Options["connection"] = conn
			}

			config.Stores[storeName] = store
			fmt.Printf("Created store: %s (%s)\n", storeName, store.Type)
		}

		return storeName, nil
	}

	// Handle single input
	if manifest.Input != nil {
		storeName, err := addStore(manifest.Input)
		if err != nil {
			return pluginDef, err
		}
		pluginDef.Input = &PluginIOSpec{
			Format: manifest.Input.Format,
			Store:  storeName,
		}
	}

	// Handle multiple inputs
	if manifest.Inputs != nil {
		pluginDef.Inputs = make(map[string]PluginIOSpec)
		for name, spec := range manifest.Inputs {
			storeName, err := addStore(spec)
			if err != nil {
				return pluginDef, err
			}
			pluginDef.Inputs[name] = PluginIOSpec{
				Format: spec.Format,
				Store:  storeName,
			}
		}
	}

	// Handle single output
	if manifest.Output != nil {
		storeName, err := addStore(manifest.Output)
		if err != nil {
			return pluginDef, err
		}
		pluginDef.Output = &PluginIOSpec{
			Format: manifest.Output.Format,
			Store:  storeName,
		}
	}

	return pluginDef, nil
}

// configureFromMetadata creates stores and plugin definition based on legacy metadata
func configureFromMetadata(config *KaloConfig, metadata *registry.PluginMetadata, version string) (PluginDefinition, error) {
	// Generate store names from format specs
	inputStoreName := formatToStoreName(metadata.InputSpec)
	outputStoreName := formatToStoreName(metadata.OutputSpec)

	// Add input store if it doesn't exist
	if _, exists := config.Stores[inputStoreName]; !exists && metadata.InputSpec != "" {
		config.Stores[inputStoreName] = Store{
			Format: metadata.InputSpec,
			Type:   StoreTypeLocalFileSystem,
			Options: map[string]any{
				"path": "$" + inputStoreName + "_PATH",
			},
		}
		fmt.Printf("Created store: %s (set %s_PATH env var)\n", inputStoreName, inputStoreName)
	}

	// Add output store if it doesn't exist and differs from input
	if outputStoreName != inputStoreName && metadata.OutputSpec != "" {
		if _, exists := config.Stores[outputStoreName]; !exists {
			config.Stores[outputStoreName] = Store{
				Format: metadata.OutputSpec,
				Type:   StoreTypeLocalFileSystem,
				Options: map[string]any{
					"path": "$" + outputStoreName + "_PATH",
				},
			}
			fmt.Printf("Created store: %s (set %s_PATH env var)\n", outputStoreName, outputStoreName)
		}
	}

	return PluginDefinition{
		Version: version,
		Input: &PluginIOSpec{
			Format: metadata.InputSpec,
			Store:  inputStoreName,
		},
		Output: &PluginIOSpec{
			Format: metadata.OutputSpec,
			Store:  outputStoreName,
		},
	}, nil
}

func parsePluginArg(arg string) (registry.PluginIdentifier, registry.PluginVersion, error) {
	// Plugin ID format: @org/name[@version]
	if !strings.HasPrefix(arg, "@") {
		return "", "", fmt.Errorf("plugin ID must start with '@'")
	}

	parts := strings.Split(arg, "@")
	if len(parts) > 3 {
		return "", "", fmt.Errorf("invalid plugin identifier format")
	}

	var id string
	var version string

	switch len(parts) {
	case 2:
		// Format: @org/name
		id = "@" + parts[1]
	case 3:
		// Format: @org/name@version
		id = "@" + parts[1]
		version = parts[2]
	default:
		return "", "", fmt.Errorf("invalid plugin identifier format")
	}

	// Validate org/name format
	nameParts := strings.Split(strings.TrimPrefix(id, "@"), "/")
	if len(nameParts) != 2 {
		return "", "", fmt.Errorf("plugin ID must be in format @org/name")
	}

	// Basic validation of org and name
	if !isValidOrgName(nameParts[0]) || !isValidPluginName(nameParts[1]) {
		return "", "", fmt.Errorf("invalid org or plugin name")
	}

	// If version is specified, validate it
	if version != "" {
		if !strings.HasPrefix(version, "v") {
			version = "v" + version
		}
		if !isValidVersion(version) {
			return "", "", fmt.Errorf("invalid version format")
		}
	}

	return registry.PluginIdentifier(id), registry.PluginVersion(version), nil
}

func isValidOrgName(name string) bool {
	// Organization name rules:
	// 1. Only lowercase letters, numbers, and hyphens
	// 2. Must start with a letter
	// 3. Must be between 2 and 32 characters
	if len(name) < 2 || len(name) > 32 {
		return false
	}
	if !unicode.IsLetter(rune(name[0])) {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' {
			return false
		}
	}
	return true
}

func isValidPluginName(name string) bool {
	// Plugin name rules:
	// 1. Only lowercase letters, numbers, and hyphens
	// 2. Must start with a letter
	// 3. Must be between 1 and 64 characters
	if len(name) < 1 || len(name) > 64 {
		return false
	}
	if !unicode.IsLetter(rune(name[0])) {
		return false
	}
	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' {
			return false
		}
	}
	return true
}

func isValidVersion(version string) bool {
	// Version must be in format v1.2.3
	if !strings.HasPrefix(version, "v") {
		return false
	}
	version = strings.TrimPrefix(version, "v")

	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return false
	}

	for _, part := range parts {
		if _, err := strconv.Atoi(part); err != nil {
			return false
		}
	}

	return true
}

// formatToStoreName converts a format spec to a store name.
// e.g., "KA:MO1:YAML1" -> "KA_MO_YAML"
//
//	"KA:MO1:PSQL1" -> "KA_MO_PSQL"
func formatToStoreName(format string) string {
	// Split by colon: ["KA", "MO1", "YAML1"]
	parts := strings.Split(format, ":")
	if len(parts) < 2 {
		return strings.ReplaceAll(format, ":", "_")
	}

	// Take org prefix and spec name, strip version numbers
	result := parts[0] // "KA"

	// For middle parts (spec identifier), strip trailing digits
	if len(parts) >= 2 {
		spec := strings.TrimRight(parts[1], "0123456789")
		result += "_" + spec
	}

	// For format part, strip trailing digits
	if len(parts) >= 3 {
		formatPart := strings.TrimRight(parts[2], "0123456789")
		result += "_" + formatPart
	}

	return result
}

func saveKaloConfig(path string, config *KaloConfig) error {
	// Marshal each section separately and join with blank lines for readability
	var sections []string

	if len(config.Stores) > 0 {
		storesData, err := yaml.Marshal(map[string]interface{}{"stores": config.Stores})
		if err != nil {
			return err
		}
		sections = append(sections, string(storesData))
	}

	if len(config.Config) > 0 {
		configData, err := yaml.Marshal(map[string]interface{}{"config": config.Config})
		if err != nil {
			return err
		}
		sections = append(sections, string(configData))
	}

	if len(config.Pipelines) > 0 {
		pipelinesData, err := yaml.Marshal(map[string]interface{}{"pipelines": config.Pipelines})
		if err != nil {
			return err
		}
		sections = append(sections, string(pipelinesData))
	}

	if len(config.Plugins) > 0 {
		pluginsData, err := yaml.Marshal(map[string]interface{}{"plugins": config.Plugins})
		if err != nil {
			return err
		}
		sections = append(sections, string(pluginsData))
	}

	// Join sections with blank lines
	content := strings.Join(sections, "\n")

	return os.WriteFile(path, []byte(content), 0644)
}

func readKaloConfig() (*KaloConfig, error) {
	data, err := os.ReadFile(KaloConfigFile)
	if err != nil {
		// Return the raw error for proper file-not-found detection
		return nil, err
	}

	var config KaloConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse kalo.yaml: %w", err)
	}

	return &config, nil
}

func readKaloLock() (*registry.LockFile, error) {
	data, err := os.ReadFile(KaloLockFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read kalo.lock: %w", err)
	}

	var lockFile registry.LockFile
	if err := yaml.Unmarshal(data, &lockFile); err != nil {
		return nil, fmt.Errorf("failed to parse kalo.lock: %w", err)
	}

	return &lockFile, nil
}

// downloadPluginFromRegistry downloads a plugin from the registry or GCS fallback
func downloadPluginFromRegistry(pluginName, version string) (string, error) {
	client := registry.NewRegistryClient(&registry.RegistryClientOptions{
		CacheDir: DefaultPluginCache,
	})

	// Use the version from kalo.yaml, default to v1.0.0 if empty
	pluginVersion := registry.PluginVersion(version)
	if pluginVersion == "" {
		pluginVersion = "v1.0.0"
	}

	downloadedPath, err := client.DownloadPlugin(registry.PluginIdentifier(pluginName), pluginVersion)
	if err != nil {
		return "", fmt.Errorf("failed to download plugin %s@%s: %w", pluginName, pluginVersion, err)
	}

	log.Printf("Downloaded plugin %s@%s to %s", pluginName, pluginVersion, downloadedPath)
	return downloadedPath, nil
}

// StoreConfig represents the configuration for a store passed to the SDK.
type StoreConfig struct {
	ID        uint32 `json:"id"`
	Type      string `json:"type"`
	MountPath string `json:"mountPath,omitempty"`

	// Git provenance (for gitRepository stores)
	GitRef       string `json:"gitRef,omitempty"`
	GitCommit    string `json:"gitCommit,omitempty"`
	GitTimestamp string `json:"gitTimestamp,omitempty"`
}

func executePlugin(
	ctx context.Context,
	wasmRuntime wazero.Runtime,
	kaloHost *hostfuncs.KaloHost,
	pluginPath string,
	stores map[string]Store,
	pluginDef PluginDefinition,
) error {
	wasmBytes, err := os.ReadFile(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugin WASM file: %w", err)
	}

	compiledWasm, err := wasmRuntime.CompileModule(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("module compile failed: %w", err)
	}

	// Build store configurations for SDK
	storeConfigs := make(map[string]StoreConfig)
	fsConfig := wazero.NewFSConfig()
	var nextStoreID uint32 = 1
	var tempDirs []string // Track temp directories for cleanup

	// Helper to configure a store
	configureStore := func(storeName, mountPath string) error {
		if storeName == "" {
			return nil
		}

		store, exists := stores[storeName]
		if !exists {
			return fmt.Errorf("store '%s' not found in kalo.yaml", storeName)
		}

		storeID := nextStoreID
		nextStoreID++

		switch store.Type {
		case StoreTypeLocalFileSystem:
			storePath := store.Path()
			if storePath == "" {
				return fmt.Errorf("store '%s': localFileSystem requires 'path' option", storeName)
			}
			storePath = os.ExpandEnv(storePath)
			fsConfig = fsConfig.WithDirMount(storePath, mountPath)
			log.Printf("Mounting store '%s' at '%s' (path: %s)", storeName, mountPath, storePath)

			storeConfigs[storeName] = StoreConfig{
				ID:        storeID,
				Type:      StoreTypeLocalFileSystem,
				MountPath: mountPath,
			}

		case StoreTypeGitRepository:
			// Checkout files from git ref to a temp directory
			repoRoot := store.GitRepoRoot()
			gitRef := store.GitRef()
			subPath := store.GitSubPath()

			log.Printf("Checking out git ref '%s' for store '%s'", gitRef, storeName)

			checkoutResult, err := checkoutGitRef(repoRoot, gitRef, subPath)
			if err != nil {
				return fmt.Errorf("failed to checkout git ref for store '%s': %w", storeName, err)
			}
			tempDirs = append(tempDirs, checkoutResult.TempDir)

			fsConfig = fsConfig.WithDirMount(checkoutResult.TempDir, mountPath)
			log.Printf("Mounting git store '%s' at '%s' (ref: %s, commit: %s)", storeName, mountPath, gitRef, checkoutResult.CommitHash[:8])

			storeConfigs[storeName] = StoreConfig{
				ID:           storeID,
				Type:         StoreTypeGitRepository,
				MountPath:    mountPath,
				GitRef:       gitRef,
				GitCommit:    checkoutResult.CommitHash,
				GitTimestamp: checkoutResult.CommitTime,
			}

		case StoreTypeCloudSqlDatabase:
			connString := store.Connection()
			if connString == "" {
				return fmt.Errorf("store '%s': cloudSqlDatabase requires 'connection' option", storeName)
			}
			connString = os.ExpandEnv(connString)
			log.Printf("Connecting to database store '%s'", storeName)

			pool, err := pgxpool.New(ctx, connString)
			if err != nil {
				return fmt.Errorf("failed to connect to database store '%s': %w", storeName, err)
			}

			kaloHost.AddConnection(storeID, pool)
			log.Printf("Database store '%s' connected (store ID: %d)", storeName, storeID)

			storeConfigs[storeName] = StoreConfig{
				ID:   storeID,
				Type: StoreTypeCloudSqlDatabase,
			}

		default:
			return fmt.Errorf("unsupported store type '%s' for store '%s'", store.Type, storeName)
		}

		return nil
	}

	// Cleanup temp directories when done
	defer func() {
		for _, dir := range tempDirs {
			os.RemoveAll(dir)
		}
	}()

	// Configure named inputs (for plugins that need multiple inputs)
	for inputName, inputSpec := range pluginDef.Inputs {
		mountPath := "/" + inputName // e.g., /base, /head
		if err := configureStore(inputSpec.Store, mountPath); err != nil {
			return err
		}
	}

	// Configure default input and output stores
	if pluginDef.Input != nil && pluginDef.Input.Store != "" {
		if err := configureStore(pluginDef.Input.Store, "/input"); err != nil {
			return err
		}
	}
	if pluginDef.Output != nil && pluginDef.Output.Store != "" {
		if err := configureStore(pluginDef.Output.Store, "/output"); err != nil {
			return err
		}
	}

	// Build plugin config for SDK
	config := map[string]any{
		"stores": storeConfigs,
		"config": pluginDef.Config,
	}

	// Add legacy paths for backward compatibility
	if pluginDef.Input != nil {
		if inputCfg, ok := storeConfigs[pluginDef.Input.Store]; ok && inputCfg.MountPath != "" {
			config["inputPath"] = inputCfg.MountPath
		}
	}
	if pluginDef.Output != nil {
		if outputCfg, ok := storeConfigs[pluginDef.Output.Store]; ok && outputCfg.MountPath != "" {
			config["outputPath"] = outputCfg.MountPath
		}
	}

	configJsonBytes, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal plugin config: %w", err)
	}

	moduleConfig := wazero.NewModuleConfig().
		WithName(pluginPath).
		WithFSConfig(fsConfig).
		WithArgs(pluginPath, string(configJsonBytes)).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr)

	pluginModule, err := wasmRuntime.InstantiateModule(ctx, compiledWasm, moduleConfig)
	if err != nil {
		return fmt.Errorf("failed to instantiate plugin module: %w", err)
	}
	defer pluginModule.Close(ctx)

	return nil
}

// GitCheckoutResult contains the result of checking out a git ref
type GitCheckoutResult struct {
	TempDir    string // Path to temp directory with extracted files
	CommitHash string // Resolved commit hash
	CommitTime string // Commit timestamp in RFC3339 format
}

// checkoutGitRef extracts files from a git ref to a temporary directory.
// It returns the checkout result including temp dir path and commit info.
func checkoutGitRef(repoRoot, refName, subPath string) (*GitCheckoutResult, error) {
	// Open the git repository
	repo, err := git.PlainOpen(repoRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to open git repository at '%s': %w", repoRoot, err)
	}

	// Resolve the reference
	var hash plumbing.Hash
	if refName == "HEAD" {
		ref, err := repo.Head()
		if err != nil {
			return nil, fmt.Errorf("failed to get HEAD: %w", err)
		}
		hash = ref.Hash()
	} else {
		// Try as branch first
		ref, err := repo.Reference(plumbing.NewBranchReferenceName(refName), true)
		if err != nil {
			// Try as tag
			ref, err = repo.Reference(plumbing.NewTagReferenceName(refName), true)
			if err != nil {
				// Try as commit hash
				hash = plumbing.NewHash(refName)
				if hash.IsZero() {
					return nil, fmt.Errorf("could not resolve ref '%s'", refName)
				}
			} else {
				hash = ref.Hash()
			}
		} else {
			hash = ref.Hash()
		}
	}

	// Get the commit
	commit, err := repo.CommitObject(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit for ref '%s': %w", refName, err)
	}

	// Get the tree
	tree, err := commit.Tree()
	if err != nil {
		return nil, fmt.Errorf("failed to get tree: %w", err)
	}

	// If subPath is specified, navigate to that subtree
	if subPath != "" {
		tree, err = tree.Tree(subPath)
		if err != nil {
			return nil, fmt.Errorf("failed to find subpath '%s' in ref '%s': %w", subPath, refName, err)
		}
	}

	// Create temp directory
	tempDir, err := os.MkdirTemp("", "kalo-git-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Extract files from the tree
	if err := extractTree(tree, tempDir); err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to extract files: %w", err)
	}

	return &GitCheckoutResult{
		TempDir:    tempDir,
		CommitHash: commit.Hash.String(),
		CommitTime: commit.Author.When.UTC().Format(time.RFC3339),
	}, nil
}

// extractTree recursively extracts files from a git tree to a directory.
func extractTree(tree *object.Tree, destDir string) error {
	for _, entry := range tree.Entries {
		destPath := filepath.Join(destDir, entry.Name)

		if entry.Mode.IsFile() {
			// Extract file
			file, err := tree.TreeEntryFile(&entry)
			if err != nil {
				return fmt.Errorf("failed to get file '%s': %w", entry.Name, err)
			}

			reader, err := file.Reader()
			if err != nil {
				return fmt.Errorf("failed to read file '%s': %w", entry.Name, err)
			}

			content, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				return fmt.Errorf("failed to read content of '%s': %w", entry.Name, err)
			}

			if err := os.WriteFile(destPath, content, 0644); err != nil {
				return fmt.Errorf("failed to write file '%s': %w", entry.Name, err)
			}
		} else {
			// It's a directory - recurse
			subTree, err := tree.Tree(entry.Name)
			if err != nil {
				return fmt.Errorf("failed to get subtree '%s': %w", entry.Name, err)
			}

			if err := os.MkdirAll(destPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory '%s': %w", entry.Name, err)
			}

			if err := extractTree(subTree, destPath); err != nil {
				return err
			}
		}
	}

	return nil
}
