
# Kalo CLI

A modern, powerful CLI tool for running Morphe compilation plugins using WebAssembly (WASM).

## Overview

Kalo CLI enables the seamless compilation of Morphe models, entities, enums, and structures across different formats using WASM plugins. The tool is designed to be extensible, supporting multiple input and output formats through a flexible configuration system.

## Features

- **WASM Plugin Support**: Run compiled plugins in a secure WASM sandbox
- **Flexible Configuration**: Define input and output specifications in a YAML configuration file
- **Environment Variable Support**: Configure plugins using environment variables
- **Multiple Format Support**: Transform between different formats (YAML, Go, PostgreSQL, TypeScript, etc.)
- **Dotenv Integration**: Load environment variables from `.env` files

## Installation

```bash
# Clone the repository
git clone https://github.com/kalo-build/kalo-cli.git
cd kalo-cli

# Build the CLI tool
go build -o kalo ./cmd/kalo
```

## Usage

```bash
# Basic usage
./kalo compile

# Specify a custom configuration file
./kalo compile -c custom-config.yaml

# Enable debug mode for verbose output
./kalo compile -d
```

## Configuration

Kalo CLI uses a YAML configuration file (`kalo.yaml` by default) to define specifications, formats, and WASM plugins. Here's an example structure:

```yaml
specs:
  "KA:MO1":
    formats:
      YAML1:
        inputMounts:
          MORPHE_MODELS_DIR_YAML: "${PWD}/morphe/yaml/models"
          # ... more input mounts
        
        outputSpecs:
          GO1:
            MORPHE_MODELS_DIR_GO: "${PWD}/morphe/go/models"
            # ... more output mounts
        
        wasmPlugins:
          - path: "./plugins/morphe-go-struct.wasm"
            inputSpec: "KA:MO1:YAML1"
            outputSpec: "KA:MO1:GO1"
            env:
              LOG_LEVEL: "debug"
```

## Building WASM Plugins

To create a WASM plugin for Kalo CLI:

```bash
# Build a Go plugin for WASM
GOOS=wasip1 GOARCH=wasm go build -o plugins/morphe-go-struct.wasm ./path/to/plugin
```

## Environment Variables

Kalo CLI supports loading environment variables from a `.env` file, which can be useful for setting up paths and other configuration values.

Example `.env` file:

```
BASE_DIR=/path/to/morphe/files
```

## License

[TBD](LICENSE)