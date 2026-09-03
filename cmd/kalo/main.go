package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	kconfig "github.com/kalo-build/kalo-cli/pkg/config"
	"github.com/kalo-build/kalo-cli/pkg/hostfuncs"
	"github.com/kalo-build/kalo-cli/pkg/registry"
	"github.com/spf13/cobra"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"gopkg.in/yaml.v3"
)

// expandEnvWithDefaults expands environment variables with bash-style default syntax.
// Supports:
//   - $VAR or ${VAR} - standard env var expansion
//   - ${VAR:-default} - use default if VAR is unset or empty
//   - ${VAR-default} - use default only if VAR is unset
func expandEnvWithDefaults(s string) string {
	// Pattern matches ${VAR:-default} or ${VAR-default}
	// Group 1: variable name
	// Group 2: :- or - (the separator)
	// Group 3: default value
	pattern := regexp.MustCompile(`\$\{([^}:]+)(:-|-)([^}]*)\}`)

	result := pattern.ReplaceAllStringFunc(s, func(match string) string {
		groups := pattern.FindStringSubmatch(match)
		if len(groups) != 4 {
			return match
		}

		varName := groups[1]
		separator := groups[2]
		defaultVal := groups[3]

		envVal, exists := os.LookupEnv(varName)

		if separator == ":-" {
			// ${VAR:-default} - use default if unset OR empty
			if !exists || envVal == "" {
				return defaultVal
			}
			return envVal
		} else {
			// ${VAR-default} - use default only if unset
			if !exists {
				return defaultVal
			}
			return envVal
		}
	})

	// Also expand regular ${VAR} and $VAR patterns
	return os.ExpandEnv(result)
}

const (
	KaloConfigFile     = "kalo.yaml"
	KaloLockFile       = "kalo.lock"
	MaxFileSize        = 200 * 1024 // 200 KB
	DefaultRegistryURL = "https://registry.kalo.build"
	DefaultPluginCache = ".kalo/plugins"
)

// Version information - injected by GoReleaser at build time
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// executionPolicy contains opt-in restrictions for plugin execution.
// The zero value preserves existing Kalo behavior except for lock hash
// verification, which is always enforced at the point of compilation.
type executionPolicy struct {
	Offline         bool
	DenyNetwork     bool
	Deterministic   bool
	ReadOnlyInputs  bool
	PluginTimeout   time.Duration
	PluginMemoryMiB uint32
}

type pluginExecutionCache struct {
	compiledModules map[string]wazero.CompiledModule
}

type pluginExecutionDiagnostic struct {
	Plugin        string
	Stage         string
	PluginVersion string
	InputFormat   string
	InputVersion  string
	Remediation   string
	Err           error
}

func (d *pluginExecutionDiagnostic) Error() string {
	return fmt.Sprintf(
		"plugin diagnostic: plugin=%q stage=%q pluginVersion=%q inputFormat=%q inputVersion=%q finding=%q remediation=%q",
		d.Plugin,
		d.Stage,
		d.PluginVersion,
		d.InputFormat,
		d.InputVersion,
		d.Err.Error(),
		d.Remediation,
	)
}

func (d *pluginExecutionDiagnostic) Unwrap() error {
	return d.Err
}

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: No .env file found. Using system environment variables.")
	}

	rootCmd := &cobra.Command{
		Use:     "kalo",
		Short:   "Kalo CLI is a tool for managing Kalo projects",
		Long:    `Kalo CLI helps you manage Kalo projects, run plugins, and more.`,
		Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, commit, date),
	}

	rootCmd.AddCommand(compileCommand())
	rootCmd.AddCommand(runCommand())
	rootCmd.AddCommand(pluginCommand())
	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(installCommand())
	rootCmd.AddCommand(specCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func compileCommand() *cobra.Command {
	var policy executionPolicy
	cmd := &cobra.Command{
		Use:   "compile",
		Short: "Run the default compile pipeline",
		Long:  `Run the default compile pipeline defined in kalo.yaml`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := runTarget("compile", policy); err != nil {
				log.Fatalf("Compilation failed: %v", err)
			}
			log.Println("Compilation completed successfully")
		},
	}

	addExecutionPolicyFlags(cmd, &policy)
	return cmd
}

func runCommand() *cobra.Command {
	var policy executionPolicy
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
			if err := runTarget(args[0], policy); err != nil {
				log.Fatalf("Run failed: %v", err)
			}
			log.Println("Run completed successfully")
		},
	}

	addExecutionPolicyFlags(cmd, &policy)
	return cmd
}

func addExecutionPolicyFlags(cmd *cobra.Command, policy *executionPolicy) {
	cmd.Flags().BoolVar(&policy.Offline, "offline", false, "Require locked plugin artifacts to already exist; never download during execution")
	cmd.Flags().BoolVar(&policy.DenyNetwork, "deny-network", false, "Deny Kalo-managed network access (registry downloads and database stores)")
	cmd.Flags().BoolVar(&policy.Deterministic, "deterministic", false, "Use deterministic host capabilities such as a stable plugin clock")
	cmd.Flags().BoolVar(&policy.ReadOnlyInputs, "read-only-inputs", false, "Mount plugin input stores read-only")
	cmd.Flags().DurationVar(&policy.PluginTimeout, "plugin-timeout", 0, "Maximum execution time per plugin (for example 2s; zero disables)")
	cmd.Flags().Uint32Var(&policy.PluginMemoryMiB, "plugin-memory-mib", 0, "Maximum memory per plugin in MiB (zero uses the wazero default)")
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

func installCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install all plugins from kalo.yaml",
		Long: `Download all plugins defined in kalo.yaml to the local cache.

This is similar to 'npm install' - it reads the existing configuration
and ensures all required plugins are downloaded locally.

If a kalo.lock file exists, plugins are downloaded at the locked versions.
If no lock file exists, one will be generated.

This command does NOT modify kalo.yaml - it only downloads plugins.

Examples:
  kalo install                    # Install all plugins from kalo.yaml
  kalo plugin install <plugin>    # Install a specific plugin (may modify kalo.yaml)`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := runInstall(); err != nil {
				log.Fatalf("Install failed: %v", err)
			}
			log.Println("Install completed successfully")
		},
	}

	return cmd
}

// runInstall downloads all plugins defined in kalo.yaml
func runInstall() error {
	// Read kalo.yaml
	config, err := readKaloConfig()
	if err != nil {
		return fmt.Errorf("failed to read kalo.yaml: %w", err)
	}

	if len(config.Plugins) == 0 {
		fmt.Println("No plugins defined in kalo.yaml")
		return nil
	}

	// Create registry client
	registryURL := os.Getenv("KALO_REGISTRY_URL")
	if registryURL == "" {
		registryURL = DefaultRegistryURL
	}

	client := registry.NewRegistryClient(&registry.RegistryClientOptions{
		RegistryURL: registryURL,
		CacheDir:    DefaultPluginCache,
		OfflineMode: false,
	})

	// Try to read existing lockfile
	lockFile, lockErr := readKaloLock()
	if lockErr != nil {
		fmt.Println("No kalo.lock found, will generate one...")
		lockFile = nil
	}

	// Download each plugin
	var downloadErrors []string
	downloadCount := 0

	for alias, pluginDef := range config.Plugins {
		pluginID := registry.PluginIdentifier(pluginDef.PluginIdentity(alias))
		version := registry.PluginVersion(pluginDef.Version)

		// Check if we have a locked version (try alias first, then plugin ID)
		if lockFile != nil {
			lockedPlugin, exists := lockFile.Plugins[registry.PluginIdentifier(alias)]
			if !exists {
				lockedPlugin, exists = lockFile.Plugins[pluginID]
			}
			if exists {
				version = lockedPlugin.Version
				if _, err := os.Stat(lockedPlugin.Location); err == nil {
					fmt.Printf("  %s (%s@%s) - already installed\n", alias, pluginID, version)
					continue
				}
			}
		}

		fmt.Printf("  %s (%s@%s) - downloading...\n", alias, pluginID, version)
		localPath, err := client.DownloadPlugin(pluginID, version)
		if err != nil {
			downloadErrors = append(downloadErrors, fmt.Sprintf("%s (%s): %v", alias, pluginID, err))
			continue
		}

		fmt.Printf("  %s (%s@%s) - installed to %s\n", alias, pluginID, version, localPath)
		downloadCount++
	}

	// Generate/update lockfile with alias-aware entries
	pluginEntries := make(map[string]registry.PluginLockEntry)
	for alias, p := range config.Plugins {
		pluginEntries[alias] = registry.PluginLockEntry{
			PluginID: registry.PluginIdentifier(p.PluginIdentity(alias)),
			Version:  registry.PluginVersion(p.Version),
		}
	}

	newLockFile, err := client.GenerateLockFileAliased(KaloConfigFile, pluginEntries)
	if err != nil {
		return fmt.Errorf("failed to generate lockfile: %w", err)
	}

	err = client.SaveLockFile(newLockFile, KaloLockFile)
	if err != nil {
		return fmt.Errorf("failed to save lockfile: %w", err)
	}

	// Report results
	fmt.Println()
	if len(downloadErrors) > 0 {
		fmt.Printf("Downloaded %d plugins with %d errors:\n", downloadCount, len(downloadErrors))
		for _, errMsg := range downloadErrors {
			fmt.Printf("  - %s\n", errMsg)
		}
		return fmt.Errorf("%d plugins failed to download", len(downloadErrors))
	}

	fmt.Printf("Installed %d plugins\n", downloadCount)
	return nil
}

// listPipelines reads kalo.yaml and displays all available pipelines
func listPipelines() error {
	// Read kalo.yaml
	configData, err := os.ReadFile("kalo.yaml")
	if err != nil {
		return fmt.Errorf("failed to read kalo.yaml: %w", err)
	}

	var config kconfig.KaloConfig
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
func runTarget(target string, policy executionPolicy) error {
	if policy.PluginMemoryMiB > 4096 {
		return fmt.Errorf("plugin memory limit %d MiB exceeds the WebAssembly maximum of 4096 MiB", policy.PluginMemoryMiB)
	}

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
		if store.Type != kconfig.StoreTypeLocalFileSystem {
			continue
		}
		storePath := store.Path()
		if storePath == "" {
			return fmt.Errorf("store %s: localFileSystem requires 'path' option", name)
		}
		storePath = expandEnvWithDefaults(storePath)
		log.Printf("Creating store directory for %s: %s", name, storePath)
		if err := os.MkdirAll(storePath, 0755); err != nil {
			return fmt.Errorf("failed to create store directory %s: %w", storePath, err)
		}
	}

	ctx := context.Background()
	runtimeConfig := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(policy.PluginTimeout > 0)
	if policy.PluginMemoryMiB > 0 {
		runtimeConfig = runtimeConfig.WithMemoryLimitPages(policy.PluginMemoryMiB * 16)
	}
	wasmRuntime := wazero.NewRuntimeWithConfig(ctx, runtimeConfig)
	defer wasmRuntime.Close(ctx)

	wasi_snapshot_preview1.MustInstantiate(ctx, wasmRuntime)

	// Create shared host functions and register once
	kaloHost := hostfuncs.NewKaloHostWithOptions(hostfuncs.HostOptions{
		Deterministic: policy.Deterministic,
	})
	defer kaloHost.Close()

	if err := kaloHost.Register(ctx, wasmRuntime); err != nil {
		return fmt.Errorf("failed to register host functions: %w", err)
	}
	executionCache := &pluginExecutionCache{
		compiledModules: make(map[string]wazero.CompiledModule),
	}

	// Check if target is a plugin (starts with @) or a pipeline
	if strings.HasPrefix(target, "@") {
		return runSinglePlugin(ctx, wasmRuntime, kaloHost, executionCache, target, config, lockFile, policy)
	}
	return runPipeline(ctx, wasmRuntime, kaloHost, executionCache, target, config, lockFile, policy)
}

// runSinglePlugin runs a single plugin by name or alias.
func runSinglePlugin(ctx context.Context, wasmRuntime wazero.Runtime, kaloHost *hostfuncs.KaloHost, executionCache *pluginExecutionCache, pluginName string, config *kconfig.KaloConfig, lockFile *registry.LockFile, policy executionPolicy) error {
	step := kconfig.StepSpec{Plugin: pluginName}
	return runPluginStep(ctx, wasmRuntime, kaloHost, executionCache, step, "single", nil, config, lockFile, policy)
}

// runPipeline runs all steps in a pipeline.
func runPipeline(ctx context.Context, wasmRuntime wazero.Runtime, kaloHost *hostfuncs.KaloHost, executionCache *pluginExecutionCache, pipelineName string, config *kconfig.KaloConfig, lockFile *registry.LockFile, policy executionPolicy) error {
	// Resolve pipeline name (check direct name first, then aliases)
	resolvedName, pipeline, exists := kconfig.ResolvePipeline(pipelineName, config.Pipelines)
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
			if err := runPluginStep(ctx, wasmRuntime, kaloHost, executionCache, step, stage.Name, stage.Config, config, lockFile, policy); err != nil {
				return fmt.Errorf("stage %q plugin %q failed: %w", stage.Name, step.Plugin, err)
			}
		}
	}

	return nil
}

// runPluginStep resolves a step's alias, applies overrides, merges config, and executes.
func runPluginStep(ctx context.Context, wasmRuntime wazero.Runtime, kaloHost *hostfuncs.KaloHost, executionCache *pluginExecutionCache, step kconfig.StepSpec, stageName string, stageConfig map[string]interface{}, config *kconfig.KaloConfig, lockFile *registry.LockFile, policy executionPolicy) (runErr error) {
	aliasOrName := step.Plugin
	log.Printf("Running plugin: %s", aliasOrName)
	diagnostic := &pluginExecutionDiagnostic{
		Plugin:        aliasOrName,
		Stage:         stageName,
		PluginVersion: "unspecified",
		InputFormat:   "unspecified",
		InputVersion:  "unspecified",
	}
	defer func() {
		if runErr == nil {
			return
		}
		diagnostic.Err = runErr
		diagnostic.Remediation = pluginRemediation(runErr)
		runErr = diagnostic
	}()

	pluginDef, pluginID, exists := kconfig.ResolvePlugin(aliasOrName, config.Plugins)
	if !exists {
		return fmt.Errorf("plugin %s not found in kalo.yaml", aliasOrName)
	}
	diagnostic.Plugin = pluginID
	diagnostic.PluginVersion = pluginDef.Version

	// Look up in lock file by alias key first, then by plugin identity
	pluginLock, exists := lockFile.Plugins[registry.PluginIdentifier(aliasOrName)]
	if !exists {
		pluginLock, exists = lockFile.Plugins[registry.PluginIdentifier(pluginID)]
		if !exists {
			return fmt.Errorf("plugin %s (identity: %s) not found in kalo.lock", aliasOrName, pluginID)
		}
	}

	// Check if plugin file exists, if not attempt to download
	pluginPath := pluginLock.Location
	if _, err := os.Stat(pluginPath); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot access locked plugin artifact %q: %w", pluginPath, err)
		}
		if policy.Offline || policy.DenyNetwork {
			return fmt.Errorf("locked plugin artifact %q is missing and network access is disabled; run 'kalo install' before restricted execution", pluginPath)
		}
		log.Printf("Plugin %s not found at %s, attempting download...", pluginID, pluginPath)
		downloadedPath, downloadErr := downloadPluginFromRegistry(pluginID, string(pluginDef.Version))
		if downloadErr != nil {
			return fmt.Errorf("plugin file not found and download failed: %w", downloadErr)
		}
		pluginPath = downloadedPath
	}

	// Apply step-level I/O overrides
	effectiveDef := kconfig.ApplyStepOverrides(pluginDef, step)
	if effectiveDef.Input != nil {
		if effectiveDef.Input.Format != "" {
			diagnostic.InputFormat = effectiveDef.Input.Format
		}
		if effectiveDef.Input.Version != "" {
			diagnostic.InputVersion = effectiveDef.Input.Version
		}
	}

	mergedConfig := kconfig.MergeConfig(config, aliasOrName, pluginID, effectiveDef.Config, stageConfig, step.Config)

	execDef := effectiveDef
	execDef.Config = mergedConfig

	pluginCtx := ctx
	cancel := func() {}
	if policy.PluginTimeout > 0 {
		pluginCtx, cancel = context.WithTimeout(ctx, policy.PluginTimeout)
	}
	defer cancel()

	err := executePlugin(pluginCtx, wasmRuntime, kaloHost, executionCache, pluginPath, pluginLock.ResolvedHash, config.Stores, execDef, policy)
	if policy.PluginTimeout > 0 && pluginCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("plugin execution exceeded timeout %s (stage %q); increase --plugin-timeout only after reviewing the plugin", policy.PluginTimeout, stageName)
	}
	return err
}

func pluginRemediation(err error) string {
	message := err.Error()
	switch {
	case strings.Contains(message, "hash mismatch"), strings.Contains(message, "no resolvedHash"):
		return "restore the reviewed artifact or regenerate kalo.lock through the approval flow"
	case strings.Contains(message, "not found in kalo.lock"):
		return "approve and lock this exact plugin identity, version, and digest before execution"
	case strings.Contains(message, "missing and network access is disabled"):
		return "run kalo install before restricted execution and review the resulting lock change"
	case strings.Contains(message, "exceeded timeout"):
		return "review the plugin for non-termination before increasing --plugin-timeout"
	case strings.Contains(message, "memory"), strings.Contains(message, "out of bounds"):
		return "review plugin allocation behavior before increasing --plugin-memory-mib"
	case strings.Contains(message, "deny-network"), strings.Contains(message, "not instantiated"):
		return "remove undeclared network capabilities or execute the operation outside the restricted compile pipeline"
	case strings.Contains(message, "store"):
		return "check the stage store declaration, access mode, and input/output compatibility"
	default:
		return "inspect the plugin manifest, kalo.lock entry, and stage configuration"
	}
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
	var aliasFlag string
	cmd := &cobra.Command{
		Use:   "install [plugin-id[@version]]",
		Short: "Install a Kalo plugin",
		Long: `Install a Kalo plugin into the current project.
The plugin will be downloaded and added to the kalo.yaml manifest.
If no version is specified, the latest version will be used.

Use --as to specify a custom alias for the plugin instance.

Example:
  kalo plugin install @kalo-build/plugin-morphe-psql-types@v1.0.0
  kalo plugin install @kalo-build/plugin-morphe-ts-types@v1.0.0 --as ts-types-ext`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPluginInstall(cmd, args, aliasFlag)
		},
	}
	cmd.Flags().StringVar(&aliasFlag, "as", "", "Custom alias for the plugin instance")

	return cmd
}

// reconcileKaloLockFile writes kalo.lock from the current kalo.yaml plugin list (alias-aware).
// Call after plugin install or when skipping work because the plugin is already present, so a
// missing or outdated lockfile is still created/updated.
func reconcileKaloLockFile(client *registry.RegistryClient, config *kconfig.KaloConfig) error {
	pluginEntries := make(map[string]registry.PluginLockEntry)
	for a, p := range config.Plugins {
		pluginEntries[a] = registry.PluginLockEntry{
			PluginID: registry.PluginIdentifier(p.PluginIdentity(a)),
			Version:  registry.PluginVersion(p.Version),
		}
	}
	lockFile, err := client.GenerateLockFileAliased(KaloConfigFile, pluginEntries)
	if err != nil {
		return fmt.Errorf("failed to generate lockfile: %w", err)
	}
	if err := client.SaveLockFile(lockFile, KaloLockFile); err != nil {
		return fmt.Errorf("failed to save lockfile: %w", err)
	}
	return nil
}

func runPluginInstall(cmd *cobra.Command, args []string, aliasFlag string) error {
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

	// Get plugin metadata (resolve endpoint returns latest if no version specified)
	metadata, err := client.GetPluginMetadata(pluginID, version)
	if err != nil {
		return fmt.Errorf("failed to get plugin metadata: %w", err)
	}

	// If no version was specified, use the resolved version from metadata
	if version == "" {
		version = metadata.Version
		if version == "" {
			return fmt.Errorf("plugin %s found but no version available", pluginID)
		}
		fmt.Printf("Resolved latest version: %s\n", version)
	}

	// Try to get plugin manifest for better configuration
	manifest, err := client.GetPluginManifest(pluginID, version)
	if err != nil {
		log.Printf("Warning: Could not fetch plugin manifest: %v", err)
	}

	// Load existing kalo.yaml or create a new one
	config, err := readKaloConfig()
	if err != nil {
		if os.IsNotExist(err) || strings.Contains(err.Error(), "cannot find the file") {
			fmt.Println("Creating new kalo.yaml...")
			config = &kconfig.KaloConfig{
				Stores:    make(map[string]kconfig.Store),
				Config:    make(map[string]interface{}),
				Pipelines: make(map[string]kconfig.Pipeline),
				Plugins:   make(map[string]kconfig.PluginDefinition),
			}
		} else {
			return fmt.Errorf("failed to load kalo.yaml: %w", err)
		}
	}

	// Determine the alias key for this plugin instance
	alias := aliasFlag
	if alias == "" {
		alias = kconfig.PluginIDToAlias(string(pluginID))
	}

	// Check if plugin is already installed (by alias or by plugin identity)
	var pluginDef kconfig.PluginDefinition
	var existingAlias string
	if existingPlugin, exists := config.Plugins[alias]; exists {
		existingAlias = alias
		if existingPlugin.Version == string(version) {
			fmt.Printf("Plugin %s@%s is already installed as '%s'.\n", pluginID, version, alias)
			fmt.Println("Nothing to do.")
			return reconcileKaloLockFile(client, config)
		}
		fmt.Printf("Updating %s (%s) from %s to %s...\n", alias, pluginID, existingPlugin.Version, version)
		pluginDef = existingPlugin
		pluginDef.Version = string(version)
	} else if foundAlias, existingPlugin, found := kconfig.FindPluginByIdentity(config.Plugins, string(pluginID)); found {
		existingAlias = foundAlias
		if existingPlugin.Version == string(version) {
			fmt.Printf("Plugin %s@%s is already installed as '%s'.\n", pluginID, version, foundAlias)
			fmt.Println("Nothing to do.")
			return reconcileKaloLockFile(client, config)
		}
		fmt.Printf("Updating %s (%s) from %s to %s...\n", foundAlias, pluginID, existingPlugin.Version, version)
		alias = foundAlias
		pluginDef = existingPlugin
		pluginDef.Version = string(version)
	} else {
		// New install
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
		pluginDef.Plugin = string(pluginID)
	}
	_ = existingAlias

	// Colocate config defaults from manifest into the plugin instance
	if pluginDef.Config == nil {
		pluginDef.Config = make(map[string]any)
	}
	if manifest != nil && manifest.ConfigSchema != nil {
		for key, option := range manifest.ConfigSchema {
			if option.Default != nil {
				if _, exists := pluginDef.Config[key]; !exists {
					pluginDef.Config[key] = option.Default
				}
			}
		}
	}

	// Add plugin to config
	if config.Plugins == nil {
		config.Plugins = make(map[string]kconfig.PluginDefinition)
	}
	config.Plugins[alias] = pluginDef

	// Create a default pipeline if none exist
	if config.Pipelines == nil {
		config.Pipelines = make(map[string]kconfig.Pipeline)
	}
	if len(config.Pipelines) == 0 {
		pipelineName := "compile"
		if manifest != nil && manifest.Modes != nil {
			for name, mode := range manifest.Modes {
				if mode.IsDefault {
					pipelineName = name
					break
				}
			}
		}

		config.Pipelines[pipelineName] = kconfig.Pipeline{
			Description: fmt.Sprintf("Run %s", pluginID),
			Stages: []kconfig.Stage{
				{
					Name:  pipelineName,
					Steps: []kconfig.StepSpec{{Plugin: alias}},
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

	if err := reconcileKaloLockFile(client, config); err != nil {
		return err
	}

	fmt.Printf("Successfully installed %s@%s as '%s' to %s\n", pluginID, version, alias, localPath)
	return nil
}

// configureFromManifest creates stores and plugin definition based on manifest
func configureFromManifest(config *kconfig.KaloConfig, manifest *registry.PluginManifest, version string) (kconfig.PluginDefinition, error) {
	pluginDef := kconfig.PluginDefinition{
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
			store := kconfig.Store{
				Format:  spec.Format,
				Type:    spec.SuggestedStore.Type,
				Options: make(map[string]any),
			}

			// Set default type if not specified
			if store.Type == "" {
				store.Type = kconfig.StoreTypeLocalFileSystem
			}

			// Configure based on store type
			switch store.Type {
			case kconfig.StoreTypeLocalFileSystem:
				path := spec.SuggestedStore.Path
				if path == "" {
					path = "./" + strings.ToLower(storeName)
				}
				store.Options["path"] = path
			case kconfig.StoreTypeGitRepository:
				store.Options["repoRoot"] = spec.SuggestedStore.RepoRoot
				store.Options["ref"] = spec.SuggestedStore.Ref
				store.Options["subPath"] = spec.SuggestedStore.SubPath
			case kconfig.StoreTypeCloudSqlDatabase:
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
		pluginDef.Input = &kconfig.PluginIOSpec{
			Format: manifest.Input.Format,
			Store:  storeName,
		}
	}

	// Handle multiple inputs
	if manifest.Inputs != nil {
		pluginDef.Inputs = make(map[string]kconfig.PluginIOSpec)
		for name, spec := range manifest.Inputs {
			storeName, err := addStore(spec)
			if err != nil {
				return pluginDef, err
			}
			pluginDef.Inputs[name] = kconfig.PluginIOSpec{
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
		pluginDef.Output = &kconfig.PluginIOSpec{
			Format: manifest.Output.Format,
			Store:  storeName,
		}
	}

	return pluginDef, nil
}

// configureFromMetadata creates stores and plugin definition based on legacy metadata
func configureFromMetadata(config *kconfig.KaloConfig, metadata *registry.PluginMetadata, version string) (kconfig.PluginDefinition, error) {
	// Generate store names from format specs
	inputStoreName := formatToStoreName(metadata.InputSpec)
	outputStoreName := formatToStoreName(metadata.OutputSpec)

	// Add input store if it doesn't exist
	if _, exists := config.Stores[inputStoreName]; !exists && metadata.InputSpec != "" {
		config.Stores[inputStoreName] = kconfig.Store{
			Format: metadata.InputSpec,
			Type:   kconfig.StoreTypeLocalFileSystem,
			Options: map[string]any{
				"path": "$" + inputStoreName + "_PATH",
			},
		}
		fmt.Printf("Created store: %s (set %s_PATH env var)\n", inputStoreName, inputStoreName)
	}

	// Add output store if it doesn't exist and differs from input
	if outputStoreName != inputStoreName && metadata.OutputSpec != "" {
		if _, exists := config.Stores[outputStoreName]; !exists {
			config.Stores[outputStoreName] = kconfig.Store{
				Format: metadata.OutputSpec,
				Type:   kconfig.StoreTypeLocalFileSystem,
				Options: map[string]any{
					"path": "$" + outputStoreName + "_PATH",
				},
			}
			fmt.Printf("Created store: %s (set %s_PATH env var)\n", outputStoreName, outputStoreName)
		}
	}

	return kconfig.PluginDefinition{
		Version: version,
		Input: &kconfig.PluginIOSpec{
			Format: metadata.InputSpec,
			Store:  inputStoreName,
		},
		Output: &kconfig.PluginIOSpec{
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

func saveKaloConfig(path string, config *kconfig.KaloConfig) error {
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

func readKaloConfig() (*kconfig.KaloConfig, error) {
	data, err := os.ReadFile(KaloConfigFile)
	if err != nil {
		// Return the raw error for proper file-not-found detection
		return nil, err
	}

	var config kconfig.KaloConfig
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

	registry.NormalizeLockFilePaths(&lockFile)
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
	executionCache *pluginExecutionCache,
	pluginPath string,
	expectedHash string,
	stores map[string]kconfig.Store,
	pluginDef kconfig.PluginDefinition,
	policy executionPolicy,
) error {
	wasmBytes, err := os.ReadFile(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugin WASM file: %w", err)
	}
	if err := verifyLockedPluginBytes(wasmBytes, expectedHash); err != nil {
		return fmt.Errorf("refusing to execute locked plugin %q: %w; restore the approved artifact or regenerate kalo.lock through the normal review flow", pluginPath, err)
	}

	cacheKey := strings.ToLower(expectedHash)
	compiledWasm, exists := executionCache.compiledModules[cacheKey]
	if !exists {
		compiledWasm, err = wasmRuntime.CompileModule(ctx, wasmBytes)
		if err != nil {
			return fmt.Errorf("module compile failed: %w", err)
		}
		executionCache.compiledModules[cacheKey] = compiledWasm
	}
	// Cached compiled modules intentionally live until the shared runtime closes.
	// This lets wazero reuse verified machine code when a pipeline invokes the
	// same locked artifact repeatedly, without extending trust beyond this run.

	// Build store configurations for SDK
	storeConfigs := make(map[string]StoreConfig)
	fsConfig := wazero.NewFSConfig()
	var nextStoreID uint32 = 1
	var tempDirs []string // Track temp directories for cleanup

	// Helper to configure a store
	configureStore := func(storeName, mountPath string, readOnly bool) error {
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
		case kconfig.StoreTypeLocalFileSystem:
			storePath := store.Path()
			if storePath == "" {
				return fmt.Errorf("store '%s': localFileSystem requires 'path' option", storeName)
			}
			storePath = expandEnvWithDefaults(storePath)
			if readOnly {
				fsConfig = fsConfig.WithReadOnlyDirMount(storePath, mountPath)
			} else {
				fsConfig = fsConfig.WithDirMount(storePath, mountPath)
			}
			log.Printf("Mounting store '%s' at '%s' (path: %s, read-only: %t)", storeName, mountPath, storePath, readOnly)

			storeConfigs[storeName] = StoreConfig{
				ID:        storeID,
				Type:      kconfig.StoreTypeLocalFileSystem,
				MountPath: mountPath,
			}

		case kconfig.StoreTypeGitRepository:
			// Checkout files from git ref to a temp directory
			repoRoot := store.GitRepoRoot()
			gitRef := expandEnvWithDefaults(store.GitRef())
			subPath := store.GitSubPath()

			log.Printf("Checking out git ref '%s' for store '%s'", gitRef, storeName)

			checkoutResult, err := checkoutGitRef(repoRoot, gitRef, subPath)
			if err != nil {
				return fmt.Errorf("failed to checkout git ref for store '%s': %w", storeName, err)
			}
			tempDirs = append(tempDirs, checkoutResult.TempDir)

			if readOnly {
				fsConfig = fsConfig.WithReadOnlyDirMount(checkoutResult.TempDir, mountPath)
			} else {
				fsConfig = fsConfig.WithDirMount(checkoutResult.TempDir, mountPath)
			}
			log.Printf("Mounting git store '%s' at '%s' (ref: %s, commit: %s, read-only: %t)", storeName, mountPath, gitRef, checkoutResult.CommitHash[:8], readOnly)

			storeConfigs[storeName] = StoreConfig{
				ID:           storeID,
				Type:         kconfig.StoreTypeGitRepository,
				MountPath:    mountPath,
				GitRef:       gitRef,
				GitCommit:    checkoutResult.CommitHash,
				GitTimestamp: checkoutResult.CommitTime,
			}

		case kconfig.StoreTypeCloudSqlDatabase:
			if policy.DenyNetwork {
				return fmt.Errorf("store '%s': cloudSqlDatabase is disabled by --deny-network", storeName)
			}
			connString := store.Connection()
			if connString == "" {
				return fmt.Errorf("store '%s': cloudSqlDatabase requires 'connection' option", storeName)
			}
			connString = expandEnvWithDefaults(connString)
			log.Printf("Connecting to database store '%s'", storeName)

			pool, err := pgxpool.New(ctx, connString)
			if err != nil {
				return fmt.Errorf("failed to connect to database store '%s': %w", storeName, err)
			}

			kaloHost.AddConnection(storeID, pool)
			log.Printf("Database store '%s' connected (store ID: %d)", storeName, storeID)

			storeConfigs[storeName] = StoreConfig{
				ID:   storeID,
				Type: kconfig.StoreTypeCloudSqlDatabase,
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
		if err := configureStore(inputSpec.Store, mountPath, policy.ReadOnlyInputs); err != nil {
			return err
		}
	}

	// Configure default input and output stores
	if pluginDef.Input != nil && pluginDef.Input.Store != "" {
		if err := configureStore(pluginDef.Input.Store, "/input", policy.ReadOnlyInputs); err != nil {
			return err
		}
	}
	if pluginDef.Output != nil && pluginDef.Output.Store != "" {
		if err := configureStore(pluginDef.Output.Store, "/output", false); err != nil {
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

func verifyLockedPluginBytes(wasmBytes []byte, expectedHash string) error {
	if expectedHash == "" {
		return fmt.Errorf("kalo.lock entry has no resolvedHash")
	}

	actualHash := fmt.Sprintf("sha256:%x", sha256.Sum256(wasmBytes))
	if !strings.EqualFold(actualHash, expectedHash) {
		return fmt.Errorf("artifact hash mismatch: lock expects %s, execution bytes are %s", expectedHash, actualHash)
	}
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
