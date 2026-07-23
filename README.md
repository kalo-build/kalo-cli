
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

### APT (Debian / Ubuntu)

Download the `.deb` package from [GitHub Releases](https://github.com/kalo-build/kalo-cli/releases) and install:

```bash
curl -LO https://github.com/kalo-build/kalo-cli/releases/latest/download/kalo_<version>_linux_amd64.deb
sudo dpkg -i kalo_<version>_linux_amd64.deb
```

### RPM (Fedora / RHEL / CentOS)

Download the `.rpm` package from [GitHub Releases](https://github.com/kalo-build/kalo-cli/releases) and install:

```bash
curl -LO https://github.com/kalo-build/kalo-cli/releases/latest/download/kalo_<version>_linux_amd64.rpm
sudo rpm -i kalo_<version>_linux_amd64.rpm
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

## Updating

To upgrade Kalo CLI to the latest release, use the same command as your installation method:

| Method | Update command |
|--------|-----------------|
| **Quick install** | Re-run the install script — it fetches the latest release |
| **Go install** | `go install github.com/kalo-build/kalo-cli/cmd/kalo@latest` |
| **Go install (specific version)** | `go install github.com/kalo-build/kalo-cli/cmd/kalo@v0.1.0` |
| **Homebrew** | `brew upgrade kalo` |
| **Scoop** | `scoop update kalo` |
| **APT (deb)** | Download the new `.deb` from [Releases](https://github.com/kalo-build/kalo-cli/releases) and `sudo dpkg -i kalo_*.deb` |
| **RPM** | Download the new `.rpm` from [Releases](https://github.com/kalo-build/kalo-cli/releases) and `sudo rpm -U kalo_*.rpm` |
| **Manual** | Download the new binary from [GitHub Releases](https://github.com/kalo-build/kalo-cli/releases) and replace your existing binary |

Verify the version:

```bash
kalo --version
```

## Usage

```bash
# Install all plugins from kalo.yaml (like npm install)
kalo install

# List available pipelines
kalo list

# Run a pipeline by name
kalo run compile

# Run a pipeline by alias (if configured)
kalo run up        # alias for migrate-up
kalo run down      # alias for migrate-down

# Run the default compile pipeline
kalo compile

# Install a specific plugin (adds to kalo.yaml if not present)
kalo plugin install @kalo-build/plugin-morphe-go-struct
```

### Commands

| Command | Description |
|---------|-------------|
| `kalo install` | Download all plugins from kalo.yaml (like `npm install`) |
| `kalo list` | List all available pipelines with descriptions |
| `kalo run <name>` | Run a pipeline or plugin by name or alias |
| `kalo compile` | Shorthand for `kalo run compile` |
| `kalo plugin install <plugin>` | Install a specific plugin from registry |

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

### `kalo.lock`

After `kalo install`, the CLI writes **`kalo.lock`** next to `kalo.yaml`. It pins each plugin’s **version**, **SHA-256** (`resolvedHash`), and the path to the cached WASM under **`.kalo/plugins/`**.

**Path separators:** On save and load, **`location`** paths are normalized to **forward slashes** (`/`). `filepath.ToSlash` alone is not enough on Linux (it only maps `os.PathSeparator`, which is already `/` there), so backslashes from Windows are replaced explicitly—same result on every OS for committed lockfiles. The OS still stores files under `.kalo/plugins/` as usual; Go resolves POSIX-style paths correctly on Windows at runtime.

**`kalo plugin install`:** A successful install or upgrade always refreshes **`kalo.lock`**. If the plugin is **already** at the requested version (`Nothing to do`), the CLI still **reconciles the lockfile** from **`kalo.yaml`** so a missing **`kalo.lock`** gets created (same as `kalo install` for that manifest).

### Restricted and deterministic execution

Kalo verifies the SHA-256 of the exact plugin bytes loaded for execution against
the plugin's `resolvedHash` in `kalo.lock`. A missing hash or mismatch fails
closed.

`kalo run` and `kalo compile` also accept opt-in restrictions for automation and
untrusted inputs:

```bash
kalo compile \
  --offline \
  --deny-network \
  --deterministic \
  --read-only-inputs \
  --plugin-timeout 2s \
  --plugin-memory-mib 32
```

- `--offline` requires every locked artifact to already be present and disables
  execution-time registry downloads.
- `--deny-network` additionally rejects database stores. WASI plugins are not
  given socket APIs.
- `--deterministic` replaces Kalo's real-time host clock with a stable value.
- `--read-only-inputs` prevents plugins from mutating input mounts; declared
  output mounts remain writable.
- `--plugin-timeout` and `--plugin-memory-mib` bound each plugin's runtime and
  linear memory.

Use `kalo install` separately before an offline run. These restrictions are
opt-in so existing projects retain their current execution behavior.

## Building WASM Plugins

To create a WASM plugin for Kalo CLI:

```bash
# Build a Go plugin for WASM
GOOS=wasip1 GOARCH=wasm go build -o plugins/morphe-go-struct.wasm ./path/to/plugin
```

## Releasing New Versions

Releases are automated via GitHub Actions using [GoReleaser](https://goreleaser.com/). To create a new release:

```bash
# Create and push a version tag
git tag v1.2.3
git push origin v1.2.3
```

This triggers the release workflow which:
1. Builds binaries for Linux, macOS, and Windows (amd64/arm64)
2. Creates a GitHub Release with the binaries, `.deb` and `.rpm` packages for Linux
3. Updates Homebrew tap and Scoop bucket formulas

### Version Format

Use [semantic versioning](https://semver.org/):
- `v1.0.0` - Major release
- `v1.1.0` - Minor release (new features)
- `v1.1.1` - Patch release (bug fixes)

### Prereleases

Tags with prerelease suffixes are automatically marked as prereleases on GitHub:

```bash
# Prerelease examples
git tag v1.0.0-alpha
git tag v1.0.0-beta.1
git tag v1.0.0-rc.1
git tag v0.0.1-dev.123
```

Prereleases:
- Are marked as "Pre-release" on GitHub Releases
- Are **not** installed by the quick install scripts (which fetch "latest")
- Are **not** pushed to Homebrew/Scoop (package managers only get stable releases)

To test a prerelease manually, download from the [Releases page](https://github.com/kalo-build/kalo-cli/releases).

## Environment Variables

Kalo CLI supports loading environment variables from a `.env` file, which can be useful for setting up paths and other configuration values.

Example `.env` file:

```
BASE_DIR=/path/to/morphe/files
```

## License

[MIT License](LICENSE)
