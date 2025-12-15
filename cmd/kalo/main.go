package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

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

// Pipeline represents a pipeline configuration
type Pipeline struct {
	Stages []Stage `yaml:"stages"`
}

// Stage represents a stage in a pipeline
type Stage struct {
	Name  string   `yaml:"name"`
	Steps []string `yaml:"steps"`
}

// PluginDefinition represents a plugin configuration
type PluginDefinition struct {
	Version string         `yaml:"version"`
	Input   PluginIOSpec   `yaml:"input"`
	Output  PluginIOSpec   `yaml:"output,omitempty"`
	Config  map[string]any `yaml:"config,omitempty"`
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

	// Check if target is a plugin (starts with @) or a pipeline
	if strings.HasPrefix(target, "@") {
		return runSinglePlugin(ctx, wasmRuntime, target, config, lockFile)
	}
	return runPipeline(ctx, wasmRuntime, target, config, lockFile)
}

// runSinglePlugin runs a single plugin by name.
func runSinglePlugin(ctx context.Context, wasmRuntime wazero.Runtime, pluginName string, config *KaloConfig, lockFile *registry.LockFile) error {
	log.Printf("Running plugin: %s", pluginName)

	pluginDef, exists := config.Plugins[pluginName]
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.yaml", pluginName)
	}

	pluginLock, exists := lockFile.Plugins[registry.PluginIdentifier(pluginName)]
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.lock", pluginName)
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

	return executePlugin(ctx, wasmRuntime, pluginLock.Location, config.Stores, pluginDef)
}

// runPipeline runs all steps in a pipeline.
func runPipeline(ctx context.Context, wasmRuntime wazero.Runtime, pipelineName string, config *KaloConfig, lockFile *registry.LockFile) error {
	pipeline, exists := config.Pipelines[pipelineName]
	if !exists {
		return fmt.Errorf("pipeline '%s' not found in kalo.yaml", pipelineName)
	}

	log.Printf("Running pipeline: %s", pipelineName)

	for _, stage := range pipeline.Stages {
		log.Printf("Running stage: %s", stage.Name)

		for _, step := range stage.Steps {
			if !strings.HasPrefix(step, "plugin: ") {
				return fmt.Errorf("invalid step format: %s (expected 'plugin: @org/name')", step)
			}
			pluginName := strings.TrimPrefix(step, "plugin: ")

			if err := runSinglePlugin(ctx, wasmRuntime, pluginName, config, lockFile); err != nil {
				return fmt.Errorf("plugin %s failed: %w", pluginName, err)
			}
		}
	}

	return nil
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

	// Load kalo.yaml
	config, err := readKaloConfig()
	if err != nil {
		return fmt.Errorf("failed to load kalo.yaml: %w", err)
	}

	// Generate store names from format specs (e.g., "KA:MO1:YAML1" -> "KA_MO_YAML")
	inputStoreName := formatToStoreName(metadata.InputSpec)
	outputStoreName := formatToStoreName(metadata.OutputSpec)

	// Create default stores if they don't exist
	if config.Stores == nil {
		config.Stores = make(map[string]Store)
	}

	// Add input store if it doesn't exist
	if _, exists := config.Stores[inputStoreName]; !exists {
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
	if outputStoreName != inputStoreName {
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

	// Add plugin to config
	if config.Plugins == nil {
		config.Plugins = make(map[string]PluginDefinition)
	}
	config.Plugins[string(pluginID)] = PluginDefinition{
		Version: string(version),
		Input: PluginIOSpec{
			Format: metadata.InputSpec,
			Store:  inputStoreName,
		},
		Output: PluginIOSpec{
			Format: metadata.OutputSpec,
			Store:  outputStoreName,
		},
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
	data, err := yaml.Marshal(config)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func readKaloConfig() (*KaloConfig, error) {
	data, err := os.ReadFile(KaloConfigFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read kalo.yaml: %w", err)
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

// StoreConfig represents the configuration for a store passed to the SDK.
type StoreConfig struct {
	ID        uint32 `json:"id"`
	Type      string `json:"type"`
	MountPath string `json:"mountPath,omitempty"`
}

func executePlugin(
	ctx context.Context,
	wasmRuntime wazero.Runtime,
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

	dbHostFuncs := hostfuncs.NewDBHostFunctions()
	defer dbHostFuncs.Close()

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

			dbHostFuncs.AddConnection(storeID, pool)
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

	// Configure input and output stores
	if err := configureStore(pluginDef.Input.Store, "/input"); err != nil {
		return err
	}
	if err := configureStore(pluginDef.Output.Store, "/output"); err != nil {
		return err
	}

	// Register host functions
	if err := dbHostFuncs.Register(ctx, wasmRuntime); err != nil {
		return fmt.Errorf("failed to register host functions: %w", err)
	}

	// Build plugin config for SDK
	config := map[string]any{
		"stores": storeConfigs,
		"config": pluginDef.Config,
	}

	// Add legacy paths for backward compatibility
	if inputCfg, ok := storeConfigs[pluginDef.Input.Store]; ok && inputCfg.MountPath != "" {
		config["inputPath"] = inputCfg.MountPath
	}
	if outputCfg, ok := storeConfigs[pluginDef.Output.Store]; ok && outputCfg.MountPath != "" {
		config["outputPath"] = outputCfg.MountPath
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
