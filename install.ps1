# Kalo CLI Installer for Windows
# Usage: irm https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.ps1 | iex
#    or: Invoke-WebRequest -Uri https://raw.githubusercontent.com/kalo-build/kalo-cli/main/install.ps1 -UseBasicParsing | Invoke-Expression

$ErrorActionPreference = "Stop"

$Repo = "kalo-build/kalo-cli"
$BinaryName = "kalo"
$InstallDir = if ($env:KALO_INSTALL_DIR) { $env:KALO_INSTALL_DIR } else { "$env:LOCALAPPDATA\kalo\bin" }

function Write-Info {
    param([string]$Message)
    Write-Host "[INFO] " -ForegroundColor Green -NoNewline
    Write-Host $Message
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] " -ForegroundColor Yellow -NoNewline
    Write-Host $Message
}

function Write-Error {
    param([string]$Message)
    Write-Host "[ERROR] " -ForegroundColor Red -NoNewline
    Write-Host $Message
    exit 1
}

function Get-LatestVersion {
    $Response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -UseBasicParsing
    return $Response.tag_name
}

function Get-Architecture {
    $Arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    switch ($Arch) {
        "X64" { return "x86_64" }
        "Arm64" { return "arm64" }
        default { Write-Error "Unsupported architecture: $Arch" }
    }
}

function Install-Kalo {
    Write-Info "Detecting system..."
    $Arch = Get-Architecture
    Write-Info "Architecture: $Arch"

    Write-Info "Fetching latest version..."
    $Version = Get-LatestVersion
    if (-not $Version) {
        Write-Error "Could not determine latest version"
    }
    Write-Info "Latest version: $Version"

    # Construct download URL
    $VersionNum = $Version.TrimStart("v")
    $Filename = "${BinaryName}_${VersionNum}_windows_${Arch}.zip"
    $DownloadUrl = "https://github.com/$Repo/releases/download/$Version/$Filename"

    Write-Info "Downloading $Filename..."
    $TempDir = New-Item -ItemType Directory -Path (Join-Path $env:TEMP "kalo-install-$(Get-Random)")
    $ZipPath = Join-Path $TempDir $Filename

    try {
        Invoke-WebRequest -Uri $DownloadUrl -OutFile $ZipPath -UseBasicParsing

        Write-Info "Extracting..."
        Expand-Archive -Path $ZipPath -DestinationPath $TempDir -Force

        Write-Info "Installing to $InstallDir..."
        if (-not (Test-Path $InstallDir)) {
            New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
        }

        $BinaryPath = Join-Path $TempDir "$BinaryName.exe"
        Copy-Item -Path $BinaryPath -Destination (Join-Path $InstallDir "$BinaryName.exe") -Force

        # Add to PATH if not already there
        $UserPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($UserPath -notlike "*$InstallDir*") {
            Write-Info "Adding $InstallDir to PATH..."
            [Environment]::SetEnvironmentVariable("Path", "$UserPath;$InstallDir", "User")
            $env:Path = "$env:Path;$InstallDir"
        }

        Write-Info "Kalo CLI installed successfully!"
        Write-Info "Run 'kalo --help' to get started"
        Write-Warn "You may need to restart your terminal for PATH changes to take effect."

        # Verify installation
        & (Join-Path $InstallDir "$BinaryName.exe") --version
    }
    finally {
        Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Install-Kalo
