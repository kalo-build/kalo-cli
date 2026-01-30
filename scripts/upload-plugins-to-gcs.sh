#!/bin/bash
# Upload built WASM plugins and manifests to GCS
# This script uploads plugins to the kalo-plugin-registry GCS bucket

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGINS_DIR="$SCRIPT_DIR/../dist/plugins"
KALO_BUILD_DIR="$(cd "$SCRIPT_DIR/../../" && pwd)"
GCS_BUCKET="gs://kalo-development-kalo-plugin-registry-plugins"

echo "Uploading plugins to GCS..."
echo "Source: $PLUGINS_DIR"
echo "Kalo build dir: $KALO_BUILD_DIR"
echo "Destination: $GCS_BUCKET"

# Check if gsutil is available
if ! command -v gsutil &> /dev/null; then
    echo "Error: gsutil not found. Install Google Cloud SDK first."
    exit 1
fi

# Check if plugins directory exists
if [ ! -d "$PLUGINS_DIR" ]; then
    echo "Error: Plugins directory not found: $PLUGINS_DIR"
    echo "Run ./scripts/build-all-plugins.sh first."
    exit 1
fi

# Upload each plugin WASM and manifest
for wasm_file in "$PLUGINS_DIR"/*.wasm; do
    if [ ! -f "$wasm_file" ]; then
        echo "No .wasm files found in $PLUGINS_DIR"
        exit 1
    fi
    
    filename=$(basename "$wasm_file")
    # Extract name and version: morphe-db-manager-v1.0.0.wasm -> @kalo-build/plugin-morphe-db-manager/v1.0.0/plugin.wasm
    # Format: name-version.wasm
    name_with_version="${filename%.wasm}"
    
    # Parse: morphe-db-manager-v1.0.0 -> name=morphe-db-manager, version=v1.0.0
    version=$(echo "$name_with_version" | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+$')
    name="${name_with_version%-$version}"
    
    gcs_wasm_path="$GCS_BUCKET/@kalo-build/plugin-$name/$version/plugin.wasm"
    gcs_manifest_path="$GCS_BUCKET/@kalo-build/plugin-$name/$version/plugin.yaml"
    
    # Upload WASM
    echo "  Uploading WASM: $filename -> $gcs_wasm_path"
    gsutil cp "$wasm_file" "$gcs_wasm_path"
    
    # Upload manifest if it exists
    manifest_file="$KALO_BUILD_DIR/plugin-$name/plugin.yaml"
    if [ -f "$manifest_file" ]; then
        echo "  Uploading manifest: plugin.yaml -> $gcs_manifest_path"
        gsutil cp "$manifest_file" "$gcs_manifest_path"
    else
        echo "  WARN: No manifest found at $manifest_file"
    fi
done

echo ""
echo "Upload complete!"
echo ""
echo "Uploaded plugins:"
gsutil ls -r "$GCS_BUCKET/@kalo-build/" 2>/dev/null || echo "  (check GCS bucket manually)"

echo ""
echo "Plugins are now available at:"
echo "  https://storage.googleapis.com/kalo-development-kalo-plugin-registry-plugins/@kalo-build/plugin-NAME/VERSION/plugin.wasm"
echo "  https://storage.googleapis.com/kalo-development-kalo-plugin-registry-plugins/@kalo-build/plugin-NAME/VERSION/plugin.yaml"
