#!/bin/bash
# Build all Kalo WASM plugins
# This script builds all plugins and prepares them for upload to the registry

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KALO_BUILD_DIR="$(cd "$SCRIPT_DIR/../../" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/../dist/plugins"

echo "Building all Kalo WASM plugins..."
echo "Kalo build directory: $KALO_BUILD_DIR"
echo "Output directory: $OUTPUT_DIR"

mkdir -p "$OUTPUT_DIR"

# List of plugins to build (plugin-dir:output-name:version)
PLUGINS=(
    "plugin-morphe-db-manager:morphe-db-manager:v1.0.0"
    "plugin-morphe-git-morphediff:morphe-git-morphediff:v1.0.0"
    "plugin-morphediff-psql:morphediff-psql:v1.0.0"
    "plugin-morphe-psql-types:morphe-psql-types:v1.0.0"
    "plugin-morphe-go-struct:morphe-go-struct:v1.0.0"
    "plugin-morphe-ts-types:morphe-ts-types:v1.0.0"
    "plugin-morphe-zod-types:morphe-zod-types:v1.0.0"
)

build_plugin() {
    local plugin_dir=$1
    local output_name=$2
    local version=$3
    local full_path="$KALO_BUILD_DIR/$plugin_dir"
    
    if [ ! -d "$full_path" ]; then
        echo "  SKIP: $plugin_dir (directory not found)"
        return 0
    fi
    
    if [ ! -f "$full_path/cmd/plugin/main.go" ]; then
        echo "  SKIP: $plugin_dir (no cmd/plugin/main.go)"
        return 0
    fi
    
    echo "  Building: $plugin_dir -> $output_name-$version.wasm"
    
    cd "$full_path"
    
    # Build WASM
    GOOS=wasip1 GOARCH=wasm go build -o "$OUTPUT_DIR/$output_name-$version.wasm" ./cmd/plugin
    
    echo "    OK: $OUTPUT_DIR/$output_name-$version.wasm"
}

echo ""
echo "Building plugins..."
for plugin_spec in "${PLUGINS[@]}"; do
    IFS=':' read -r plugin_dir output_name version <<< "$plugin_spec"
    build_plugin "$plugin_dir" "$output_name" "$version"
done

echo ""
echo "Build complete!"
echo ""
echo "Built plugins:"
ls -la "$OUTPUT_DIR"/*.wasm 2>/dev/null || echo "  (none)"

echo ""
echo "Next step: Upload to GCS with:"
echo "  ./scripts/upload-plugins-to-gcs.sh"
