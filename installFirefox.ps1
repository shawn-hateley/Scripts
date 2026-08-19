#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Downloads and silently installs the latest version of Mozilla Firefox.

.DESCRIPTION
    Pulls the current full offline installer directly from Mozilla's official
    redirector, verifies the download, and runs a silent install.
    Safe to re-run: if Firefox is already installed, Mozilla's installer
    will simply update it in place.

.PARAMETER Language
    Firefox locale code (default: en-US). Examples: de, fr, es-ES, ja.

.PARAMETER Architecture
    win64 or win32 (default: win64).

.EXAMPLE
    .\Install-Firefox.ps1

.EXAMPLE
    .\Install-Firefox.ps1 -Language de -Architecture win64
#>

[CmdletBinding()]
param(
    [string]$Language = "en-US",
    [ValidateSet("win64", "win32")]
    [string]$Architecture = "win64"
)

$ErrorActionPreference = "Stop"

function Write-Status {
    param([string]$Message)
    Write-Host "[Install-Firefox] $Message" -ForegroundColor Cyan
}

try {
    # Mozilla's official "latest" redirector always points at the current release.
    $downloadUrl = "https://download.mozilla.org/?product=firefox-latest&os=$Architecture&lang=$Language"
    $installerPath = Join-Path $env:TEMP "FirefoxSetup.exe"

    Write-Status "Downloading latest Firefox ($Architecture, $Language)..."
    Invoke-WebRequest -Uri $downloadUrl -OutFile $installerPath -UseBasicParsing

    if (-not (Test-Path $installerPath) -or (Get-Item $installerPath).Length -lt 1MB) {
        throw "Download failed or file is too small to be a valid installer."
    }
    Write-Status "Downloaded to $installerPath"

    Write-Status "Running silent install..."
    $process = Start-Process -FilePath $installerPath -ArgumentList "/S" -Wait -PassThru

    if ($process.ExitCode -ne 0) {
        throw "Installer exited with code $($process.ExitCode)."
    }

    Write-Status "Firefox installed successfully."

    Write-Status "Cleaning up installer file..."
    Remove-Item $installerPath -Force -ErrorAction SilentlyContinue

    # Quick sanity check for a common install location.
    $exePaths = @(
        "$env:ProgramFiles\Mozilla Firefox\firefox.exe",
        "${env:ProgramFiles(x86)}\Mozilla Firefox\firefox.exe"
    )
    $found = $exePaths | Where-Object { Test-Path $_ }
    if ($found) {
        Write-Status "Verified: $($found | Select-Object -First 1)"
    } else {
        Write-Warning "Install reported success but firefox.exe wasn't found in the expected location. You may want to check manually."
    }
}
catch {
    Write-Error "Firefox installation failed: $_"
    exit 1
}