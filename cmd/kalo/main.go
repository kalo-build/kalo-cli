package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"github.com/kalo-build/go-util/strcase"
	"github.com/kalo-build/kalo-cli/pkg/registry"
	"github.com/spf13/cobra"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"gopkg.in/yaml.v3"
)

type StoreConfig struct {
	Type       string            `yaml:"type"` // e.g., "localFileSystem", "s3", "database"
	Parameters map[string]string `yaml:"parameters,omitempty"`
}

type KaloConfig struct {
	Stores    map[string]StoreConfig     `yaml:"stores,omitempty"`
	Pipelines map[string][]PipelineStage `yaml:"pipelines,omitempty"`
	Plugins   map[string]PluginConfig    `yaml:"plugins,omitempty"`
}

type PipelineStage struct {
	Stage string   `yaml:"stage"`
	Steps []string `yaml:"steps"`
}

type PluginInput struct {
	Spec  string `yaml:"spec"`
	Store string `yaml:"store"`
}

type PluginOutput struct {
	Spec  string `yaml:"spec"`
	Store string `yaml:"store"`
}

type PluginConfig struct {
	Version string         `yaml:"version"`
	Input   PluginInput    `yaml:"input"`
	Output  PluginOutput   `yaml:"output"`
	Config  map[string]any `yaml:"config,omitempty"`
}

func main() {
	// Load .env file if present
	godotenv.Load()

	// Initialize registry client
	registryClient := registry.NewRegistryClient(nil)

	// Root command
	rootCmd := &cobra.Command{
		Use:   "kalo",
		Short: "Kalo CLI - A modern tool for running Morphe compilation plugins",
		Long: `Kalo CLI is a powerful and flexible tool for compiling Morphe models, entities, 
enums, and structures across different formats using WASM plugins.

It takes a kalo.yaml configuration file that defines pipelines and plugins,
and uses WebAssembly modules to perform the compilation.`,
		Run: func(cmd *cobra.Command, args []string) {
			// If no command is specified, print help
			cmd.Help()
		},
	}

	// Compile command
	var compileConfigPath string
	var debugMode bool

	compileCmd := &cobra.Command{
		Use:   "compile",
		Short: "Compile using WASM plugins according to kalo.yaml specification",
		Run: func(cmd *cobra.Command, args []string) {
			config, err := loadConfig(compileConfigPath)
			if err != nil {
				log.Fatalf("Error loading config: %v", err)
			}

			if debugMode {
				prettyPrintConfig(config)
			}

			// Load or generate lockfile
			lockFile, err := ensureLockFile(registryClient, config, compileConfigPath)
			if err != nil {
				log.Fatalf("Error managing lockfile: %v", err)
			}

			// Run compilation with the lockfile
			err = runCompilation(config, lockFile, debugMode)
			if err != nil {
				log.Fatalf("Compilation failed: %v", err)
			}

			fmt.Println("Compilation completed successfully.")
		},
	}

	compileCmd.Flags().StringVarP(&compileConfigPath, "config", "c", "kalo.yaml", "Path to kalo.yaml configuration file")
	compileCmd.Flags().BoolVarP(&debugMode, "debug", "d", false, "Enable debug mode with verbose output")

	// Plugin command group
	pluginCmd := &cobra.Command{
		Use:   "plugin",
		Short: "Manage Kalo plugins",
	}

	// Plugin add command
	var (
		pluginAddVersion     string
		pluginAddInputSpec   string
		pluginAddOutputSpec  string
		pluginAddInputStore  string
		pluginAddOutputStore string
	)

	pluginAddCmd := &cobra.Command{
		Use:   "add [plugin-id]",
		Short: "Add a new plugin to kalo.yaml",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			pluginID := args[0]

			config, err := loadConfig("kalo.yaml")
			if err != nil {
				if !os.IsNotExist(err) {
					log.Fatalf("Error loading config: %v", err)
				}
				config = &KaloConfig{
					Plugins: make(map[string]PluginConfig),
					Stores:  make(map[string]StoreConfig),
				}
			}

			if config.Plugins == nil {
				config.Plugins = make(map[string]PluginConfig)
			}
			if config.Stores == nil {
				config.Stores = make(map[string]StoreConfig)
			}

			if _, exists := config.Plugins[pluginID]; exists {
				log.Fatalf("Plugin %s already exists in configuration", pluginID)
			}

			if pluginAddVersion == "" {
				resolvedVersion, err := registryClient.ResolveVersion(registry.PluginIdentifier(pluginID), "latest")
				if err != nil {
					log.Fatalf("Failed to resolve latest version for %s: %v", pluginID, err)
				}
				pluginAddVersion = string(resolvedVersion)
				fmt.Printf("Resolved version for %s to %s", pluginID, pluginAddVersion)
			}

			inputSpec := pluginAddInputSpec
			outputSpec := pluginAddOutputSpec
			if inputSpec == "" || outputSpec == "" {
				metadata, err := registryClient.GetPluginMetadata(
					registry.PluginIdentifier(pluginID),
					registry.PluginVersion(pluginAddVersion),
				)
				if err == nil {
					if inputSpec == "" {
						inputSpec = metadata.InputSpec
						fmt.Printf("Using default input spec from registry: %s", inputSpec)
					}
					if outputSpec == "" {
						outputSpec = metadata.OutputSpec
						fmt.Printf("Using default output spec from registry: %s", outputSpec)
					}
				} else {
					log.Printf("Warning: Could not fetch plugin metadata for defaults: %v", err)
				}
			}

			if inputSpec == "" {
				log.Fatalf("Input specification (--input-spec) is required")
			}
			if outputSpec == "" {
				log.Fatalf("Output specification (--output-spec) is required")
			}
			if pluginAddInputStore == "" {
				log.Fatalf("Input store (--input-store) is required")
			}
			if pluginAddOutputStore == "" {
				log.Fatalf("Output store (--output-store) is required")
			}

			pluginConfig := PluginConfig{
				Version: pluginAddVersion,
				Input: PluginInput{
					Spec:  inputSpec,
					Store: pluginAddInputStore,
				},
				Output: PluginOutput{
					Spec:  outputSpec,
					Store: pluginAddOutputStore,
				},
				Config: make(map[string]interface{}),
			}

			config.Plugins[pluginID] = pluginConfig

			err = saveConfig(config, "kalo.yaml")
			if err != nil {
				log.Fatalf("Failed to save config: %v", err)
			}

			fmt.Printf("Added plugin %s@%s to kalo.yaml", pluginID, pluginAddVersion)
			fmt.Printf("  Input:  Spec=%s, Store=%s", pluginConfig.Input.Spec, pluginConfig.Input.Store)
			fmt.Printf("  Output: Spec=%s, Store=%s", pluginConfig.Output.Spec, pluginConfig.Output.Store)
			fmt.Println("Remember to configure any necessary store details in kalo.yaml and set environment variables.")
		},
	}

	pluginAddCmd.Flags().StringVar(&pluginAddVersion, "version", "", "Plugin version (default: latest)")
	pluginAddCmd.Flags().StringVar(&pluginAddInputSpec, "input-spec", "", "Input specification (e.g., KA:MO1:YAML1)")
	pluginAddCmd.Flags().StringVar(&pluginAddOutputSpec, "output-spec", "", "Output specification (e.g., KA:MO1:PSQL1)")
	pluginAddCmd.Flags().StringVar(&pluginAddInputStore, "input-store", "", "Name of the store defined in kalo.yaml for input")
	pluginAddCmd.Flags().StringVar(&pluginAddOutputStore, "output-store", "", "Name of the store defined in kalo.yaml for output")

	// Plugin config command
	var pluginConfigSet []string

	pluginConfigCmd := &cobra.Command{
		Use:   "config [plugin-id]",
		Short: "Configure a plugin in kalo.yaml",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			pluginID := args[0]

			config, err := loadConfig("kalo.yaml")
			if err != nil {
				log.Fatalf("Error loading config: %v", err)
			}

			plugin, exists := config.Plugins[pluginID]
			if !exists {
				log.Fatalf("Plugin %s not found in configuration", pluginID)
			}

			for _, setArg := range pluginConfigSet {
				key, value, found := strings.Cut(setArg, "=")
				if !found {
					log.Fatalf("Invalid --set format: %s (expected key=value)", setArg)
				}

				key = strings.TrimSpace(key)
				value = strings.TrimSpace(value)

				switch {
				case key == "version":
					plugin.Version = value
				case key == "input.spec":
					plugin.Input.Spec = value
				case key == "input.store":
					plugin.Input.Store = value
				case key == "output.spec":
					plugin.Output.Spec = value
				case key == "output.store":
					plugin.Output.Store = value
				case strings.HasPrefix(key, "config."):
					configPath := strings.TrimPrefix(key, "config.")
					if plugin.Config == nil {
						plugin.Config = make(map[string]interface{})
					}
					parts := strings.Split(configPath, ".")
					current := plugin.Config
					for i, part := range parts {
						if i == len(parts)-1 {
							current[part] = value
						} else {
							if _, ok := current[part]; !ok {
								current[part] = make(map[string]interface{})
							}
							nestedMap, ok := current[part].(map[string]interface{})
							if !ok {
								log.Fatalf("Config path segment '%s' in '%s' is not a map", part, key)
							}
							current = nestedMap
						}
					}
				default:
					log.Printf("Warning: Setting arbitrary top-level keys like '%s' is deprecated. Use 'config.%s' instead.", key, key)
					if plugin.Config == nil {
						plugin.Config = make(map[string]interface{})
					}
					plugin.Config[key] = value
				}
			}

			config.Plugins[pluginID] = plugin

			err = saveConfig(config, "kalo.yaml")
			if err != nil {
				log.Fatalf("Failed to save config: %v", err)
			}

			fmt.Printf("Updated configuration for plugin %s", pluginID)
		},
	}

	pluginConfigCmd.Flags().StringArrayVar(&pluginConfigSet, "set", []string{}, "Set a configuration value (key=value)")

	// Plugin install command
	var pluginInstallOffline bool
	var pluginInstallForce bool

	pluginInstallCmd := &cobra.Command{
		Use:   "install",
		Short: "Install or update plugin WASM binaries",
		Run: func(cmd *cobra.Command, args []string) {
			// Load config
			config, err := loadConfig("kalo.yaml")
			if err != nil {
				log.Fatalf("Error loading config: %v", err)
			}

			if pluginInstallOffline {
				registryClient = registry.NewRegistryClient(&registry.RegistryClientOptions{
					OfflineMode: true,
				})
			}

			// Prepare plugin versions map for lockfile generation
			pluginVersions := make(map[registry.PluginIdentifier]registry.PluginVersion)
			for id, plugin := range config.Plugins {
				pluginVersions[registry.PluginIdentifier(id)] = registry.PluginVersion(plugin.Version)
			}

			// Generate or update lockfile
			lockFile, err := registryClient.GenerateLockFile("kalo.yaml", pluginVersions)
			if err != nil {
				log.Fatalf("Failed to generate lockfile: %v", err)
			}

			// Save lockfile
			err = registryClient.SaveLockFile(lockFile, "kalo.lock")
			if err != nil {
				log.Fatalf("Failed to save lockfile: %v", err)
			}

			fmt.Println("Plugins installed successfully.")
		},
	}

	pluginInstallCmd.Flags().BoolVar(&pluginInstallOffline, "offline", false, "Use offline mode (no downloads)")
	pluginInstallCmd.Flags().BoolVar(&pluginInstallForce, "force", false, "Force reinstallation of plugins")

	// Add commands to root
	pluginCmd.AddCommand(pluginAddCmd, pluginConfigCmd, pluginInstallCmd)
	rootCmd.AddCommand(compileCmd, pluginCmd)

	// Execute root command
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// loadConfig loads the kalo.yaml configuration file
func loadConfig(path string) (*KaloConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config KaloConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// saveConfig saves the kalo.yaml configuration file
func saveConfig(config *KaloConfig, path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Add header comment
	content := "# Kalo YAML Configuration\n\n" + string(data)

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// ensureLockFile loads or generates the lockfile
func ensureLockFile(client *registry.RegistryClient, config *KaloConfig, configPath string) (*registry.LockFile, error) {
	lockPath := strings.TrimSuffix(configPath, filepath.Ext(configPath)) + ".lock"

	// Try to load existing lockfile
	lockFile, err := client.LoadLockFile(lockPath)
	if err == nil {
		return lockFile, nil
	}

	// Generate new lockfile if it doesn't exist
	pluginVersions := make(map[registry.PluginIdentifier]registry.PluginVersion)
	for id, plugin := range config.Plugins {
		pluginVersions[registry.PluginIdentifier(id)] = registry.PluginVersion(plugin.Version)
	}

	lockFile, err = client.GenerateLockFile(configPath, pluginVersions)
	if err != nil {
		return nil, fmt.Errorf("failed to generate lockfile: %w", err)
	}

	// Save the lockfile
	err = client.SaveLockFile(lockFile, lockPath)
	if err != nil {
		return nil, fmt.Errorf("failed to save lockfile: %w", err)
	}

	return lockFile, nil
}

// prettyPrintConfig prints the configuration in a readable format
func prettyPrintConfig(config *KaloConfig) {
	fmt.Println("Loaded configuration:")
	fmt.Println("=====================")

	// Print pipelines
	if len(config.Pipelines) > 0 {
		fmt.Println("Pipelines:")
		for name, stages := range config.Pipelines {
			fmt.Printf("  %s:\n", name)
			for _, stage := range stages {
				fmt.Printf("    Stage: %s\n", stage.Stage)
				fmt.Println("    Steps:")
				for _, step := range stage.Steps {
					fmt.Printf("      - %s\n", step)
				}
			}
		}
		fmt.Println()
	}

	// Print plugins
	if len(config.Plugins) > 0 {
		fmt.Println("Plugins:")
		for id, plugin := range config.Plugins {
			fmt.Printf("  %s:\n", id)
			fmt.Printf("    Version: %s\n", plugin.Version)
			if plugin.Input.Spec != "" {
				fmt.Printf("    Input Spec: %s\n", plugin.Input.Spec)
			}
			if plugin.Output.Spec != "" {
				fmt.Printf("    Output Spec: %s\n", plugin.Output.Spec)
			}
			if len(plugin.Config) > 0 {
				fmt.Println("    Config:")
				prettyPrintMap(plugin.Config, 6)
			}
			fmt.Println()
		}
	}
}

// prettyPrintMap prints a map with the specified indentation
func prettyPrintMap(m map[string]interface{}, indent int) {
	indentStr := strings.Repeat(" ", indent)
	for k, v := range m {
		if nestedMap, ok := v.(map[string]interface{}); ok {
			fmt.Printf("%s%s:\n", indentStr, k)
			prettyPrintMap(nestedMap, indent+2)
		} else {
			fmt.Printf("%s%s: %v\n", indentStr, k, v)
		}
	}
}

// runCompilation runs the compilation process
func runCompilation(config *KaloConfig, lockFile *registry.LockFile, debug bool) error {
	// Create a new WebAssembly Runtime
	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)
	defer runtime.Close(ctx)

	// Instantiate WASI
	wasi_snapshot_preview1.MustInstantiate(ctx, runtime)

	// Get default pipeline
	var pipelineStages []PipelineStage
	if p, ok := config.Pipelines["compile"]; ok {
		pipelineStages = p
	} else {
		return fmt.Errorf("no 'compile' pipeline found in configuration")
	}

	// Execute each stage in the pipeline
	for _, stage := range pipelineStages {
		if debug {
			fmt.Printf("Executing stage: %s\n", stage.Stage)
		}

		// Execute each step in the stage
		for _, step := range stage.Steps {
			// Parse the step to determine its type
			stepParts := strings.SplitN(step, ": ", 2)
			if len(stepParts) != 2 {
				return fmt.Errorf("invalid step format: %s", step)
			}

			stepType, stepValue := stepParts[0], stepParts[1]

			switch stepType {
			case "plugin":
				// Run a plugin
				pluginID := stepValue
				plugin, ok := config.Plugins[pluginID]
				if !ok {
					return fmt.Errorf("plugin not found: %s", pluginID)
				}

				// Get plugin path from lockfile
				pluginLockInfo, ok := lockFile.Plugins[registry.PluginIdentifier(pluginID)]
				if !ok {
					return fmt.Errorf("plugin not found in lockfile: %s", pluginID)
				}

				if debug {
					fmt.Printf("Executing plugin: %s@%s\n", pluginID, plugin.Version)
				}

				// Execute the plugin
				err := executePlugin(ctx, runtime, plugin, pluginLockInfo.Location, debug)
				if err != nil {
					return fmt.Errorf("failed to execute plugin %s: %w", pluginID, err)
				}

			case "cmd":
				// Run a command
				if debug {
					fmt.Printf("Executing command: %s\n", stepValue)
				}
				// TODO: Implement command execution

			default:
				return fmt.Errorf("unknown step type: %s", stepType)
			}
		}
	}

	return nil
}

// executePlugin executes a WASM plugin
func executePlugin(ctx context.Context, runtime wazero.Runtime, plugin PluginConfig, pluginPath string, debug bool) error {
	// Read the WebAssembly module
	wasmBytes, err := os.ReadFile(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read wasm file: %w", err)
	}

	// Create a temporary directory structure for WASI
	dirStructure, err := createWasiDirectoryStructure(plugin)
	if err != nil {
		return fmt.Errorf("failed to create directory structure: %w", err)
	}

	// Prepare environment variables
	env := prepareEnvironmentVariables(plugin)

	// Configure the module
	moduleConfig := wazero.NewModuleConfig().
		WithArgs("kalo")

	// Set environment variables
	for k, v := range env {
		moduleConfig = moduleConfig.WithEnv(k, v)
	}

	// Mount directories
	fsConfig := wazero.NewFSConfig()
	for hostPath, guestPath := range dirStructure {
		if debug {
			fmt.Printf("  Mounting %s -> %s\n", hostPath, guestPath)
		}
		fsConfig = fsConfig.WithDirMount(hostPath, guestPath)
	}
	moduleConfig = moduleConfig.WithFSConfig(fsConfig)

	// Compile the WebAssembly module
	compiledModule, err := runtime.CompileModule(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("failed to compile wasm module: %w", err)
	}

	// Instantiate the module
	instance, err := runtime.InstantiateModule(ctx, compiledModule, moduleConfig)
	if err != nil {
		return fmt.Errorf("failed to instantiate wasm module: %w", err)
	}
	defer instance.Close(ctx)

	// Get the exit code
	exitCode, err := instance.ExportedFunction("_start").Call(ctx)
	if err != nil {
		return fmt.Errorf("error executing wasm module: %w", err)
	}

	if exitCode[0] != 0 {
		return fmt.Errorf("plugin exited with code: %d", exitCode[0])
	}

	return nil
}

// createWasiDirectoryStructure creates the directory structure for a plugin
func createWasiDirectoryStructure(plugin PluginConfig) (map[string]string, error) {
	dirMap := make(map[string]string)

	// Handle the main directory
	if dirPath, ok := plugin.Config["dirPath"].(string); ok {
		// Ensure directory exists
		resolvedPath := os.ExpandEnv(dirPath)
		if err := os.MkdirAll(resolvedPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", resolvedPath, err)
		}

		// Mount the directory
		dirMap[resolvedPath] = "/output"
	}

	// Parse input specification
	inputSpec := parseSpecification(plugin.Input.Spec)
	outputSpec := parseSpecification(plugin.Output.Spec)

	// Setup input directories based on input specification
	if inputSpec.Format != "" {
		inputDir, err := setupInputDirectory(inputSpec)
		if err != nil {
			return nil, fmt.Errorf("failed to setup input directory: %w", err)
		}

		// Mount the input directory
		dirMap[inputDir] = "/input"

		// Add standard model subdirectories if they exist
		for _, subdir := range []string{"models", "entities", "enums", "structures"} {
			subdirPath := filepath.Join(inputDir, subdir)
			if _, err := os.Stat(subdirPath); err == nil {
				dirMap[subdirPath] = filepath.Join("/input", subdir)
			}
		}
	}

	// Setup output directory based on output specification and config
	if outputSpec.Format != "" {
		outputDir, err := setupOutputDirectory(outputSpec, plugin.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to setup output directory: %w", err)
		}

		// Mount the output directory
		dirMap[outputDir] = "/output"

		// Add standard output subdirectories based on the format
		setupOutputSubdirectories(outputDir, outputSpec, dirMap)
	}

	return dirMap, nil
}

// parseSpecification parses a specification string like "KA:MO1:YAML1"
func parseSpecification(spec string) SpecInfo {
	parts := strings.Split(spec, ":")
	if len(parts) != 3 {
		return SpecInfo{}
	}

	return SpecInfo{
		Organization: parts[0],
		ModelVersion: parts[1],
		Format:       parts[2],
	}
}

// SpecInfo represents a parsed specification
type SpecInfo struct {
	Organization string
	ModelVersion string
	Format       string
}

// setupInputDirectory creates and returns the appropriate input directory
func setupInputDirectory(spec SpecInfo) (string, error) {
	var inputDir string

	// Determine directory based on format
	switch {
	case strings.HasPrefix(spec.Format, "YAML"):
		inputDir = "./morphe/yaml"
	case strings.HasPrefix(spec.Format, "JSON"):
		inputDir = "./morphe/json"
	default:
		// Default to a format-specific directory
		format := strings.ToLower(spec.Format)
		inputDir = fmt.Sprintf("./morphe/%s", format)
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create input directory %s: %w", inputDir, err)
	}

	return inputDir, nil
}

// setupOutputDirectory creates and returns the appropriate output directory
func setupOutputDirectory(spec SpecInfo, config map[string]interface{}) (string, error) {
	// Check if dirPath is explicitly set in the config
	if dirPath, ok := config["dirPath"].(string); ok {
		// Resolve environment variables
		resolvedPath := os.ExpandEnv(dirPath)

		// Create the directory if it doesn't exist
		if err := os.MkdirAll(resolvedPath, 0755); err != nil {
			return "", fmt.Errorf("failed to create output directory %s: %w", resolvedPath, err)
		}

		return resolvedPath, nil
	}

	// Determine default directory based on format
	var outputDir string
	switch {
	case strings.HasPrefix(spec.Format, "PSQL"):
		outputDir = "./output/sql"
	case strings.HasPrefix(spec.Format, "GO"):
		outputDir = "./output/go"
	case strings.HasPrefix(spec.Format, "TS"):
		outputDir = "./output/ts"
	default:
		// Default to a format-specific directory
		format := strings.ToLower(spec.Format)
		outputDir = fmt.Sprintf("./output/%s", format)
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
	}

	return outputDir, nil
}

// setupOutputSubdirectories creates format-specific subdirectories
func setupOutputSubdirectories(baseDir string, spec SpecInfo, dirMap map[string]string) {
	// Create standard subdirectories based on format
	subdirs := []string{}

	switch {
	case strings.HasPrefix(spec.Format, "PSQL"):
		subdirs = []string{"models", "entities", "enums", "structures"}
	case strings.HasPrefix(spec.Format, "GO"):
		subdirs = []string{"models", "entities", "enums", "structures"}
	case strings.HasPrefix(spec.Format, "TS"):
		subdirs = []string{"models", "entities", "enums", "structures"}
	}

	// Create subdirectories and add to dirMap
	for _, subdir := range subdirs {
		subdirPath := filepath.Join(baseDir, subdir)
		if err := os.MkdirAll(subdirPath, 0755); err == nil {
			dirMap[subdirPath] = filepath.Join("/output", subdir)
		}
	}
}

// prepareEnvironmentVariables prepares environment variables for a plugin
func prepareEnvironmentVariables(plugin PluginConfig) map[string]string {
	env := make(map[string]string)

	// Add configuration as environment variables
	addConfigAsEnv("", plugin.Config, env)

	// Add standard environment variables
	env["INPUT_SPEC"] = plugin.Input.Spec
	env["OUTPUT_SPEC"] = plugin.Output.Spec
	env["PLUGIN_VERSION"] = plugin.Version

	return env
}

// addConfigAsEnv adds config values as environment variables
func addConfigAsEnv(prefix string, config map[string]interface{}, env map[string]string) {
	for k, v := range config {
		key := k
		if prefix != "" {
			key = prefix + "_" + key
		}

		// Convert to uppercase and snake case
		key = strings.ToUpper(strcase.ToSnakeCase(key))

		// Handle nested maps
		if nestedMap, ok := v.(map[string]interface{}); ok {
			addConfigAsEnv(key, nestedMap, env)
		} else {
			// Convert value to string
			var strValue string
			switch val := v.(type) {
			case string:
				strValue = val
			case bool:
				if val {
					strValue = "true"
				} else {
					strValue = "false"
				}
			case float64:
				strValue = fmt.Sprintf("%g", val)
			default:
				data, _ := json.Marshal(val)
				strValue = string(data)
			}
			env[key] = strValue
		}
	}
}
