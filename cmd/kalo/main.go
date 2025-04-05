package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"gopkg.in/yaml.v3"
)

type KaloConfig struct {
	Specs map[string]SpecConfig `yaml:"specs"`
}

type SpecConfig struct {
	Formats map[string]FormatConfig `yaml:"formats"`
}

type FormatConfig struct {
	InputMounts map[string]string            `yaml:"inputMounts"`
	OutputSpecs map[string]map[string]string `yaml:"outputSpecs"`
	WasmPlugins []WasmPlugin                 `yaml:"wasmPlugins,omitempty"`
}

type WasmPlugin struct {
	Path       string            `yaml:"path"`
	InputSpec  string            `yaml:"inputSpec"`
	OutputSpec string            `yaml:"outputSpec"`
	Env        map[string]string `yaml:"env,omitempty"`
}

func main() {
	// Load .env file if present
	godotenv.Load()

	var configPath string
	var debugMode bool

	rootCmd := &cobra.Command{
		Use:   "kalo",
		Short: "Kalo CLI - A modern tool for running Morphe compilation plugins",
		Long: `Kalo CLI is a powerful and flexible tool for compiling Morphe models, entities, 
enums, and structures across different formats using WASM plugins.

It takes a kalo.yaml configuration file that defines input and output specifications,
and uses WebAssembly modules to perform the compilation.`,
		Run: func(cmd *cobra.Command, args []string) {
			// If no command is specified, print help
			cmd.Help()
		},
	}

	compileCmd := &cobra.Command{
		Use:   "compile",
		Short: "Compile using WASM plugins according to kalo.yaml specification",
		Run: func(cmd *cobra.Command, args []string) {
			config, err := loadConfig(configPath)
			if err != nil {
				log.Fatalf("Error loading config: %v", err)
			}

			if debugMode {
				prettyPrintConfig(config)
			}

			err = runCompilation(config, debugMode)
			if err != nil {
				log.Fatalf("Compilation failed: %v", err)
			}

			fmt.Println("Compilation completed successfully.")
		},
	}

	compileCmd.Flags().StringVarP(&configPath, "config", "c", "kalo.yaml", "Path to kalo.yaml configuration file")
	compileCmd.Flags().BoolVarP(&debugMode, "debug", "d", false, "Enable debug mode with verbose output")

	rootCmd.AddCommand(compileCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func loadConfig(path string) (*KaloConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config KaloConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

func prettyPrintConfig(config *KaloConfig) {
	fmt.Println("Loaded configuration:")
	fmt.Println("=====================")

	for specName, spec := range config.Specs {
		fmt.Printf("Spec: %s\n", specName)

		for formatName, format := range spec.Formats {
			fmt.Printf("  Format: %s\n", formatName)

			fmt.Println("    Input Mounts:")
			for mountName, mountPath := range format.InputMounts {
				fmt.Printf("      %s -> %s\n", mountName, mountPath)
			}

			fmt.Println("    Output Specs:")
			for outSpecName, outMounts := range format.OutputSpecs {
				fmt.Printf("      %s:\n", outSpecName)
				for mountName, mountPath := range outMounts {
					fmt.Printf("        %s -> %s\n", mountName, mountPath)
				}
			}

			if len(format.WasmPlugins) > 0 {
				fmt.Println("    WASM Plugins:")
				for _, plugin := range format.WasmPlugins {
					fmt.Printf("      Path: %s\n", plugin.Path)
					fmt.Printf("      Input Spec: %s\n", plugin.InputSpec)
					fmt.Printf("      Output Spec: %s\n", plugin.OutputSpec)
					if len(plugin.Env) > 0 {
						fmt.Println("      Environment:")
						for k, v := range plugin.Env {
							fmt.Printf("        %s=%s\n", k, v)
						}
					}
				}
			}
		}
		fmt.Println()
	}
}

func runCompilation(config *KaloConfig, debug bool) error {
	// Create a new WebAssembly Runtime
	ctx := context.Background()
	runtime := wazero.NewRuntime(ctx)
	defer runtime.Close(ctx)

	// Instantiate WASI
	wasi_snapshot_preview1.MustInstantiate(ctx, runtime)

	// Find all plugins that need to be executed
	for specName, spec := range config.Specs {
		for formatName, format := range spec.Formats {
			if len(format.WasmPlugins) == 0 {
				if debug {
					fmt.Printf("No plugins defined for %s:%s\n", specName, formatName)
				}
				continue
			}

			// Process all plugins for this format
			for _, plugin := range format.WasmPlugins {
				err := executeWasmPlugin(ctx, runtime, config, specName, formatName, plugin, debug)
				if err != nil {
					return fmt.Errorf("failed to execute plugin %s: %w", plugin.Path, err)
				}
			}
		}
	}

	return nil
}

func executeWasmPlugin(ctx context.Context, runtime wazero.Runtime, config *KaloConfig,
	specName, formatName string, plugin WasmPlugin, debug bool) error {

	if debug {
		fmt.Printf("Executing plugin: %s\n", plugin.Path)
	}

	// Read the WebAssembly module
	wasmBytes, err := os.ReadFile(plugin.Path)
	if err != nil {
		return fmt.Errorf("failed to read wasm file: %w", err)
	}

	// Get input format configuration
	inputFormatParts := strings.Split(plugin.InputSpec, ":")
	if len(inputFormatParts) != 3 {
		return fmt.Errorf("invalid input spec format: %s", plugin.InputSpec)
	}
	inputSpec, inputFormat := inputFormatParts[0]+":"+inputFormatParts[1], inputFormatParts[2]

	// Get output format configuration
	outputFormatParts := strings.Split(plugin.OutputSpec, ":")
	if len(outputFormatParts) != 3 {
		return fmt.Errorf("invalid output spec format: %s", plugin.OutputSpec)
	}
	outputSpec, outputFormat := outputFormatParts[0]+":"+outputFormatParts[1], outputFormatParts[2]

	// Verify the specs exist in the config
	if _, ok := config.Specs[inputSpec]; !ok {
		return fmt.Errorf("input spec %s not found in config", inputSpec)
	}

	if _, ok := config.Specs[inputSpec].Formats[inputFormat]; !ok {
		return fmt.Errorf("input format %s not found in spec %s", inputFormat, inputSpec)
	}

	if _, ok := config.Specs[outputSpec]; !ok {
		return fmt.Errorf("output spec %s not found in config", outputSpec)
	}

	if _, ok := config.Specs[outputSpec].Formats[outputFormat]; !ok {
		return fmt.Errorf("output format %s not found in spec %s", outputFormat, outputSpec)
	}

	// Configure environment variables for the WASM module
	env := make([]string, 0)

	// Add input mount environment variables
	inputMounts := config.Specs[inputSpec].Formats[inputFormat].InputMounts
	for mountName, mountPath := range inputMounts {
		// Resolve path from environment variable if needed
		resolvedPath := os.ExpandEnv(mountPath)
		env = append(env, fmt.Sprintf("%s=%s", mountName, resolvedPath))

		// Ensure directory exists
		if err := os.MkdirAll(resolvedPath, 0755); err != nil {
			return fmt.Errorf("failed to create input directory %s: %w", resolvedPath, err)
		}
	}

	// Add output mount environment variables
	outputMounts := config.Specs[outputSpec].Formats[outputFormat].OutputSpecs[outputFormat]
	for mountName, mountPath := range outputMounts {
		// Resolve path from environment variable if needed
		resolvedPath := os.ExpandEnv(mountPath)
		env = append(env, fmt.Sprintf("%s=%s", mountName, resolvedPath))

		// Ensure directory exists
		if err := os.MkdirAll(resolvedPath, 0755); err != nil {
			return fmt.Errorf("failed to create output directory %s: %w", resolvedPath, err)
		}
	}

	// Add custom environment variables from plugin configuration
	for name, value := range plugin.Env {
		env = append(env, fmt.Sprintf("%s=%s", name, os.ExpandEnv(value)))
	}

	if debug {
		fmt.Println("Plugin environment variables:")
		for _, envVar := range env {
			fmt.Printf("  %s\n", envVar)
		}
	}

	// Configure the module
	moduleConfig := wazero.NewModuleConfig().
		WithArgs("kalo")

	for _, envVar := range env {
		parts := strings.SplitN(envVar, "=", 2)
		if len(parts) == 2 {
			moduleConfig = moduleConfig.WithEnv(parts[0], parts[1])
		}
	}

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
