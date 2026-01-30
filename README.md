
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

### Quick Install (Recommended)

**macOS / Linux:**
```bash
curl -fsSL https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.sh | sh
```

**Windows (PowerShell):**
```powershell
irm https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.ps1 | iex
```

### Homebrew (macOS / Linux)

```bash
# Install directly (auto-taps the repo)
brew install kalo-build/tap/kalo

# Or manually tap first
brew tap kalo-build/tap
brew install kalo
```

### Scoop (Windows)

```powershell
# Add the bucket (one-time)
scoop bucket add kalo https://github.com/kalo-build/scoop-bucket

# Install
scoop install kalo
```

### Go Install

```bash
go install github.com/kalo-build/kalo-cli/cmd/kalo@latest
```

### Manual Download

Download the latest release from [GitHub Releases](https://github.com/kalo-build/kalo-cli/releases).

### Build from Source

```bash
git clone https://github.com/kalo-build/kalo-cli.git
cd kalo-cli
go build -o kalo ./cmd/kalo
```

## Usage

```bash
# List available pipelines
./kalo list

# Run a pipeline by name
./kalo run compile

# Run a pipeline by alias (if configured)
./kalo run up        # alias for migrate-up
./kalo run down      # alias for migrate-down

# Run the default compile pipeline
./kalo compile
```

### Commands

| Command | Description |
|---------|-------------|
| `kalo list` | List all available pipelines with descriptions |
| `kalo run <name>` | Run a pipeline or plugin by name or alias |
| `kalo compile` | Shorthand for `kalo run compile` |
| `kalo plugin install` | Install plugins from registry |

## Configuration

Kalo CLI uses a YAML configuration file (`kalo.yaml` by default) to define stores, plugins, and pipelines.

### Stores

Stores define data sources and destinations:

```yaml
stores:
  # Local filesystem store
  KA_MIGRATIONS:
    format: "KA:PSQL:MIGRATION1"
    type: "localFileSystem"
    options:
      path: "./migrations"

  # Git repository store (extracts files from a git ref)
  KA_GIT_MAIN:
    format: "KA:MO1:YAML1"
    type: "gitRepository"
    options:
      repoRoot: "."
      ref: "main"
      subPath: "morphe/registry"

  # Cloud SQL database store
  DB_MAIN:
    format: "KA:PSQL:LIVE"
    type: "cloudSqlDatabase"
    options:
      provider: "gcp"
      connection: "$DATABASE_URL"
```

### Pipelines

Pipelines define multi-stage workflows:

```yaml
pipelines:
  compile:
    description: "Compile Morphe schemas to PSQL and Go"
    stages:
    - name: "psql-types"
      steps:
        - "plugin: @kalo-build/plugin-morphe-psql-types"

  migrate-up:
    description: "Apply pending migrations"
    alias: "up"  # Enables: kalo run up
    stages:
    - name: "up"
      steps:
        - "plugin: @kalo-build/plugin-morphe-db-manager"
      config:
        mode: "up"
```

### Plugins

Configure plugin inputs, outputs, and settings:

```yaml
plugins:
  "@kalo-build/plugin-morphe-db-manager":
    version: "v1.0.0"
    inputs:
      schema:
        format: "KA:MO1:PSQL1"
        store: "KA_MO_PSQL"
      migrations:
        format: "KA:PSQL:MIGRATION1"
        store: "KA_MIGRATIONS"
    output:
      format: "KA:PSQL:LIVE"
      store: "DB_MAIN"
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