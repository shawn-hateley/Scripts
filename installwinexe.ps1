#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Installs a Windows .exe installer, optionally silently.

.DESCRIPTION
    Generic wrapper around Start-Process for running .exe installers.
    Most installers accept a silent/unattended switch, but the switch
    varies by installer type (NSIS, InnoSetup, InstallShield, MSI-wrapped, etc).
    Common ones are listed below - pick the one that matches your installer,
    or run it with no arguments to do an interactive install.

.PARAMETER Path
    Full path to the .exe installer.

.PARAMETER Arguments
    Command-line arguments to pass to the installer (e.g. "/S", "/VERYSILENT", "/quiet").
    Leave blank to run interactively.

.PARAMETER LogFile
    Optional path to capture install output, if the installer supports logging.

.EXAMPLE
    .\Install-Exe.ps1 -Path "C:\Downloads\Setup.exe" -Arguments "/S"

.EXAMPLE
    .\Install-Exe.ps1 -Path "C:\Downloads\Setup.exe" -Arguments "/VERYSILENT /NORESTART" -LogFile "C:\Temp\install.log"

.NOTES
    Common silent-install switches by installer framework:
        NSIS            /S
        Inno Setup      /VERYSILENT /NORESTART /SUPPRESSMSGBOXES
        InstallShield   /s /v"/qn"
        WiX Burn        /quiet /norestart
        Squirrel        --silent
    If unsure which framework built the installer, try running
    "Setup.exe /?" or "Setup.exe /help" first to see supported flags.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateScript({
        if (-not (Test-Path $_ -PathType Leaf)) {
            throw "File not found: $_"
        }
        if ((Get-Item $_).Extension -ne ".exe") {
            throw "Path must point to an .exe file."
        }
        $true
    })]
    [string]$Path,

    [string]$Arguments = "",

    [string]$LogFile
)

$ErrorActionPreference = "Stop"

function Write-Status {
    param([string]$Message)
    Write-Host "[Install-Exe] $Message" -ForegroundColor Cyan
}

try {
    $fullPath = (Resolve-Path $Path).Path
    Write-Status "Installer: $fullPath"

    $startArgs = @{
        FilePath     = $fullPath
        Wait         = $true
        PassThru     = $true
    }

    if ($Arguments) {
        Write-Status "Arguments: $Arguments"
        $startArgs["ArgumentList"] = $Arguments
    } else {
        Write-Status "No arguments supplied - installer will run interactively."
    }

    Write-Status "Starting install..."
    $process = Start-Process @startArgs

    Write-Status "Installer exited with code $($process.ExitCode)."

    # 0 = success on almost everything. 3010 = success, reboot required (common on MSI-based installers).
    if ($process.ExitCode -eq 0) {
        Write-Status "Install completed successfully."
    }
    elseif ($process.ExitCode -eq 3010) {
        Write-Status "Install completed successfully. A reboot is required."
    }
    else {
        Write-Warning "Installer returned a non-standard exit code ($($process.ExitCode)). This may or may not indicate failure - check the vendor's documentation for this installer's exit codes."
    }

    if ($LogFile -and (Test-Path $LogFile)) {
        Write-Status "Log written to $LogFile"
    }
}
catch {
    Write-Error "Installation failed: $_"
    exit 1
}