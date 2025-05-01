package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/kalo-build/kalo-cli/pkg/registry"
	"github.com/spf13/cobra"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
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

// Store represents a data store configuration
type Store struct {
	Format string `yaml:"format"`
	Type   string `yaml:"type"`
	Path   string `yaml:"path"`
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
	Output  PluginIOSpec   `yaml:"output"`
	Config  map[string]any `yaml:"config,omitempty"`
}

// PluginIOSpec represents a plugin's input or output specification
type PluginIOSpec struct {
	Format string `yaml:"format"`
	Store  string `yaml:"store"`
}

const (
	KaloConfigFile = "kalo.yaml"
	KaloLockFile   = "kalo.lock"
	MaxFileSize    = 200 * 1024 // 200 KB
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
			// Read kalo.yaml
			config, err := readKaloConfig()
			if err != nil {
				log.Fatalf("Failed to read kalo.yaml: %v", err)
			}

			// Read kalo.lock
			lockFile, err := readKaloLock()
			if err != nil {
				log.Fatalf("Failed to read kalo.lock: %v", err)
			}

			// Ensure all store directories exist
			for _, store := range config.Stores {
				storePath := os.ExpandEnv(store.Path)
				log.Println("Making store directory", store.Path, storePath)
				if err := os.MkdirAll(storePath, 0755); err != nil {
					log.Fatalf("Failed to create store directory %s: %v", storePath, err)
				}
			}

			// Run the default compile pipeline
			pipeline, exists := config.Pipelines["compile"]
			if !exists {
				log.Fatalf("Compile pipeline not found in kalo.yaml")
			}

			// Setup WASM runtime
			ctx := context.Background()
			wasmRuntime := wazero.NewRuntime(ctx)
			defer wasmRuntime.Close(ctx)

			wasi_snapshot_preview1.MustInstantiate(ctx, wasmRuntime)

			// Run each stage in the pipeline
			for _, stage := range pipeline.Stages {
				log.Printf("Running stage: %s", stage.Name)

				for _, step := range stage.Steps {
					// Parse step to get plugin name
					// Example format: "plugin: @kalo-build/plugin-morphe-psql-types"
					pluginName := step[8:] // Remove "plugin: " prefix

					log.Printf("Running step: %s", pluginName)

					// Get plugin definition
					pluginDef, exists := config.Plugins[pluginName]
					if !exists {
						log.Fatalf("Plugin %s not found in kalo.yaml", pluginName)
					}

					// Get plugin location from lock file
					pluginLock, exists := lockFile.Plugins[registry.PluginIdentifier(pluginName)]
					if !exists {
						log.Fatalf("Plugin %s not found in kalo.lock", pluginName)
					}

					// Lookup plugin config if exists
					pluginConfig, pluginConfigExists := config.Config[pluginName]
					if pluginConfigExists {
						pluginDef.Config = pluginConfig.(map[string]any)
					}

					// Execute plugin
					err := executePlugin(ctx, wasmRuntime, pluginLock.Location, config.Stores, pluginDef)
					if err != nil {
						log.Fatalf("Failed to execute plugin %s: %v", pluginName, err)
					}
				}
			}

			log.Println("Compilation completed successfully")
		},
	}

	return cmd
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

func executePlugin(
	ctx context.Context,
	wasmRuntime wazero.Runtime,
	pluginPath string,
	stores map[string]Store,
	pluginDef PluginDefinition,
) error {
	// Read the plugin WASM file
	wasmBytes, err := os.ReadFile(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugin WASM file: %w", err)
	}

	compiledWasm, err := wasmRuntime.CompileModule(ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("module compile failed: %w", err)
	}

	// Create a host module with the AppendToFile function
	hostModule, err := wasmRuntime.NewHostModuleBuilder("env").
		NewFunctionBuilder().
		WithFunc(appendToFileHostFunc).
		WithParameterNames("path_ptr", "path_len", "content_ptr", "content_len").
		WithResultNames("errno").
		Export("AppendToFile").
		Instantiate(ctx)

	if err != nil {
		return fmt.Errorf("failed to instantiate host module: %w", err)
	}
	defer hostModule.Close(ctx)

	inputStore, inputStoreExists := stores[pluginDef.Input.Store]
	outputStore, outputStoreExists := stores[pluginDef.Output.Store]

	if !inputStoreExists {
		return fmt.Errorf("input store '%s' not found in kalo.yaml", pluginDef.Input.Store)
	}
	if !outputStoreExists {
		return fmt.Errorf("output store '%s' not found in kalo.yaml", pluginDef.Output.Store)
	}

	inputStorePath := os.ExpandEnv(inputStore.Path)
	outputStorePath := os.ExpandEnv(outputStore.Path)

	fsConfig := wazero.NewFSConfig().
		WithDirMount(inputStorePath, "/input").
		WithDirMount(outputStorePath, "/output")

	log.Printf("Mounting host input store '%s' at '/input'", inputStorePath)
	log.Printf("Mounting host output store '%s' at '/output'", outputStorePath)

	config := map[string]any{
		"inputPath":  "/input",
		"outputPath": "/output",
		"config":     pluginDef.Config,
	}

	// Marshal plugin config to JSON
	configJsonBytes, jsonErr := json.Marshal(config)
	if jsonErr != nil {
		return fmt.Errorf("failed to marshal plugin config: %w", jsonErr)
	}

	// Configure WASM module
	moduleConfig := wazero.NewModuleConfig().
		WithName(pluginPath).
		WithFSConfig(fsConfig).
		WithArgs(pluginPath, string(configJsonBytes)).
		WithStdout(os.Stdout).
		WithStderr(os.Stderr)

	// Compile, instantiate, and run the plugin module via _initialize -> _start -> main()
	pluginModule, err := wasmRuntime.InstantiateModule(ctx, compiledWasm, moduleConfig)
	if err != nil {
		return fmt.Errorf("failed to instantiate plugin module: %w", err)
	}
	defer pluginModule.Close(ctx)

	return nil
}

// appendToFileHostFunc implements the host function that WASM modules can call
// It matches the expected TinyGo/Rust signature used in WASM modules
func appendToFileHostFunc(ctx context.Context, m api.Module, pathPtr, pathLen, contentPtr, contentLen uint32) uint32 {
	// Read path string from WASM memory
	pathBytes, ok := m.Memory().Read(pathPtr, pathLen)
	if !ok {
		log.Printf("Failed to read path from WASM memory")
		return 1 // Error code
	}
	pathStr := string(pathBytes)

	// Read content bytes from WASM memory
	contentBytes, ok := m.Memory().Read(contentPtr, contentLen)
	if !ok {
		log.Printf("Failed to read content from WASM memory")
		return 1 // Error code
	}

	// Check file size limit
	fi, statErr := os.Stat(pathStr)
	fileSize := 0
	if statErr == nil { // File exists
		fileSize = int(fi.Size())
	}

	if fileSize+len(contentBytes) > MaxFileSize {
		log.Printf("File size would exceed max limit of %d bytes", MaxFileSize)
		return 1 // Error code
	}

	// Append to file
	f, openErr := os.OpenFile(pathStr, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if openErr != nil {
		log.Printf("Failed to open file %s: %v", pathStr, openErr)
		return 1 // Error code
	}
	defer f.Close()

	if _, writeErr := f.Write(contentBytes); writeErr != nil {
		log.Printf("Failed to write to file %s: %v", pathStr, writeErr)
		return 1 // Error code
	}

	return 0 // Success
}
