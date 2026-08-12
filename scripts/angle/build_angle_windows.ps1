<#
.SYNOPSIS
    Build ANGLE's libEGL/libGLESv2 for Windows at a pinned revision and package them the way
    travel-animator-shared's `third_party/angle/PROVENANCE.md` packages the iOS slices.

.DESCRIPTION
    RUN THIS BY HAND, ONCE PER REVISION, ON A WINDOWS BOX. Deliberately NOT wired to a workflow:
    CI never builds ANGLE, it downloads a SHA-256-pinned release asset (see
    `.github/workflows/publish-tada-wheel.yml`). Read PROVENANCE-windows.md beside this file
    first -- toolchain requirements, the d3dcompiler_47.dll finding, verification and publish.

    Every platform must be built from ONE pinned revision: two ANGLE builds means two shader
    translators and two rasterisations of the same frame, i.e. a golden-frame parity problem.
    The revision is not a knob to turn casually.

.PARAMETER Revision
    ANGLE revision to build. Must be the same 40-hex revision every other platform is built from.

.PARAMETER TargetCpu
    `x64` or `arm64`. Only `x64` has a corresponding wheel today.

.PARAMETER IncludeD3dCompiler
    'true' to ship `d3dcompiler_47.dll` alongside the ANGLE pair, 'false' to build it, assert it,
    and then leave it out. A licensing decision, not a technical one, hence a parameter.

.PARAMETER WorkDir
    Scratch root for depot_tools and the ANGLE checkout. Roughly 25 GB.

.PARAMETER OutDir
    Where the finished zip, its SHA-256 and the provenance JSON are written.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$Revision,

    [ValidateSet('x64', 'arm64')]
    [string]$TargetCpu = 'x64',

    # Deliberately a string, not [bool]/[switch]: `-IncludeD3dCompiler false` bound to a [bool]
    # arrives as the non-empty string "false" and converts to $true, silently inverting the flag.
    [ValidateSet('true', 'false')]
    [string]$IncludeD3dCompiler = 'true',

    [string]$WorkDir = $(if ($env:RUNNER_TEMP) { Join-Path $env:RUNNER_TEMP 'angle-build' } else { Join-Path $PWD 'angle-build' }),

    [string]$OutDir = $(Join-Path $PWD 'angle-out')
)

$ErrorActionPreference = 'Stop'

# PowerShell 7.4+ applies 'Stop' to NATIVE commands too, throwing a generic "Program failed" before
# Invoke-Native can name the step that died. Off, in favour of the explicit exit-code checks below.
$PSNativeCommandUseErrorActionPreference = $false

# PowerShell does not fail a script when a native command exits non-zero; the GitHub `pwsh` shell
# only propagates the LAST command's code. Every external call therefore goes through here.
function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$What,
        [Parameter(Mandatory = $true)][string]$Command,
        [string[]]$Arguments = @()
    )
    Write-Host "==> $What"
    Write-Host "    $Command $($Arguments -join ' ')"
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$What failed (exit $LASTEXITCODE): $Command $($Arguments -join ' ')"
    }
}

function New-CleanDirectory {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (Test-Path -LiteralPath $Path) { Remove-Item -LiteralPath $Path -Recurse -Force }
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

$shortRevision = $Revision.Substring(0, 8)
$outSubdir = "out/win-$TargetCpu"

New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null
New-Item -ItemType Directory -Force -Path $OutDir  | Out-Null

# ---------------------------------------------------------------------------------------------
# 1. The three environment facts a Chromium-family checkout needs on Windows.
# ---------------------------------------------------------------------------------------------
#
# Left unset, `gn gen` tries to download Google's INTERNAL packaged MSVC toolchain from a
# Google-only bucket and fails with an access error that reads like a network problem. `0` means
# "use the Visual Studio already installed on this machine".
$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'

# Chromium-family checkouts contain paths past Windows' 260-character limit; without this,
# `gclient sync` dies with "Filename too long" twenty minutes into the downloads.
Invoke-Native 'git: allow long paths' 'git' @('config', '--global', 'core.longpaths', 'true')

# A CRLF-mangled .py or .gn file is a syntax error, not a diff.
Invoke-Native 'git: disable autocrlf' 'git' @('config', '--global', 'core.autocrlf', 'false')

# ---------------------------------------------------------------------------------------------
# 2. depot_tools.
# ---------------------------------------------------------------------------------------------
#
# Not pinned, on purpose: depot_tools has no release tags and self-updates on first use anyway. It
# is the only unpinned input -- ANGLE and everything `gclient sync` fetches come from the DEPS.
$depotTools = Join-Path $WorkDir 'depot_tools'
if (-not (Test-Path -LiteralPath $depotTools)) {
    Invoke-Native 'clone depot_tools' 'git' @(
        'clone', '--depth', '1',
        'https://chromium.googlesource.com/chromium/tools/depot_tools.git',
        $depotTools
    )
}
# PREPENDED: depot_tools' own python3/ninja/gn wrappers must win over the runner's. The `.bat`
# entry points are the supported ones on Windows, hence named explicitly below rather than PATHEXT.
$env:PATH = "$depotTools;$env:PATH"

Invoke-Native 'bootstrap depot_tools' (Join-Path $depotTools 'gclient.bat') @('--version')

# ---------------------------------------------------------------------------------------------
# 3. ANGLE at the pinned revision.
# ---------------------------------------------------------------------------------------------
#
# A full clone. Shallow is tempting, but `gclient sync` resolves DEPS against the checked-out
# commit and a `--depth 1` fetch of an arbitrary revision is exactly the case that breaks.
$angle = Join-Path $WorkDir 'angle'
if (-not (Test-Path -LiteralPath $angle)) {
    Invoke-Native 'clone ANGLE' 'git' @(
        'clone', 'https://chromium.googlesource.com/angle/angle', $angle
    )
}
Push-Location $angle
try {
    Invoke-Native 'checkout the pinned revision' 'git' @('checkout', '--detach', $Revision)

    # Prefer depot_tools' `python3.bat` -- it is the interpreter every other tool in this checkout
    # is about to use, and a bare `python3` is not guaranteed to resolve at all on Windows.
    $python = Join-Path $depotTools 'python3.bat'
    if (-not (Test-Path -LiteralPath $python)) { $python = 'python' }
    Invoke-Native 'bootstrap the gclient solution' $python @('scripts/bootstrap.py')
    Invoke-Native 'gclient sync' (Join-Path $depotTools 'gclient.bat') @('sync', '--no-history', '-D')

    # -----------------------------------------------------------------------------------------
    # 4. gn args -- the Windows/D3D11 counterpart of PROVENANCE.md's iOS/Metal block.
    # -----------------------------------------------------------------------------------------
    #
    # Two derived defaults in `gni/angle.gni` are why the "off" switches earn their lines:
    #   angle_enable_swiftshader = angle_enable_vulkan && ... -- leaving Vulkan on drags in a whole
    #     second rasterizer, i.e. a second set of pixels if it ever got packaged.
    #   angle_enable_hlsl = angle_enable_d3d11 -- the D3D11 backend's shader translator comes along
    #     automatically. Do NOT set angle_enable_hlsl here; setting a derived arg is how they drift.
    # angle_enable_gl (WGL passthrough) and angle_enable_null are second driver paths that must not
    # ship; forcing gl false unconditionally keeps one args block for both CPUs.
    $argsGn = @"
# Generated by scripts/angle/build_angle_windows.ps1 -- do not hand-edit in the out dir.
target_os = "win"
target_cpu = "$TargetCpu"
is_debug = false
is_component_build = false

angle_enable_d3d11 = true
angle_enable_vulkan = false
angle_enable_gl = false
angle_enable_metal = false
angle_enable_null = false

symbol_level = 1
"@

    New-CleanDirectory (Join-Path $angle $outSubdir)
    Set-Content -LiteralPath (Join-Path $angle "$outSubdir/args.gn") -Value $argsGn -Encoding ascii
    Write-Host "--- args.gn ---`n$argsGn`n---------------"

    Invoke-Native 'gn gen' (Join-Path $depotTools 'gn.bat') @('gen', $outSubdir)

    # -----------------------------------------------------------------------------------------
    # 5. Build -- and name `copy_compiler_dll` explicitly.
    # -----------------------------------------------------------------------------------------
    #
    # `d3dcompiler_47.dll` is not compiled from source: ANGLE's `copy_compiler_dll` target copies it
    # out of the Windows SDK's Redist/D3D folder and hangs it off libANGLE_no_vulkan as a data_dep.
    # Naming the target makes the DLL a build PRODUCT with a build failure attached, rather than a
    # file that either shows up or does not.
    Invoke-Native 'autoninja' (Join-Path $depotTools 'autoninja.bat') @(
        '-C', $outSubdir, 'libEGL', 'libGLESv2', 'copy_compiler_dll'
    )
}
finally {
    Pop-Location
}

# ---------------------------------------------------------------------------------------------
# 6. Assert what came out, before packaging anything.
# ---------------------------------------------------------------------------------------------
$built = Join-Path $angle $outSubdir

foreach ($name in @('libEGL.dll', 'libGLESv2.dll')) {
    $path = Join-Path $built $name
    if (-not (Test-Path -LiteralPath $path)) {
        throw "$name is missing from $built. The build reported success, so this is a gn " +
              "args/target-name problem, not a compile failure -- check that the target is still " +
              "called '$($name -replace '\.dll$','')' in ANGLE's root BUILD.gn at $Revision."
    }
}

# Asserted whether or not we ship it: an absent file means the SDK's Redist/D3D folder is missing
# from the image, which would silently change what a future -IncludeD3dCompiler true run produces.
$d3dCompiler = Join-Path $built 'd3dcompiler_47.dll'
if (-not (Test-Path -LiteralPath $d3dCompiler)) {
    throw @"
d3dcompiler_47.dll is missing from $built.

It is not built from source -- ANGLE's `copy_compiler_dll` target copies it out of
  <windows_sdk_path>/Redist/D3D/$TargetCpu/d3dcompiler_47.dll
so an absent file means the installed Windows SDK has no D3D redistributable component (or the
target was renamed upstream). Install the Windows SDK's "Windows SDK for UWP Managed Apps" /
redistributable component on the builder, or re-check ANGLE's BUILD.gn at $Revision.

This matters because ANGLE's libGLESv2.dll resolves the HLSL compiler at RUN time by name --
d3dcompiler_47.dll, then _46, then _43, via LoadLibrary; it is neither a static nor a delay-load
import -- so a missing copy does not fail to load, it fails to compile a shader, much later.
"@
}
$d3dVersion = (Get-Item -LiteralPath $d3dCompiler).VersionInfo.FileVersion

# ---------------------------------------------------------------------------------------------
# 7. Stage the distribution.
# ---------------------------------------------------------------------------------------------
#
# `build_jvm_payload.py --angle-dir` iterates top-level FILES only, so the zip ROOT is exactly what
# reaches the wheel's flat `_jvm/angle/` and `include/`+`lib/` stay out of it. LICENSE sits at the
# root on purpose: BSD-3-Clause wants the notice to travel with the binaries it covers.
$dist = Join-Path $WorkDir "angle-dist-win-$TargetCpu"
New-CleanDirectory $dist
New-Item -ItemType Directory -Force -Path (Join-Path $dist 'include') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dist 'lib') | Out-Null

Copy-Item -LiteralPath (Join-Path $built 'libEGL.dll')    -Destination $dist
Copy-Item -LiteralPath (Join-Path $built 'libGLESv2.dll') -Destination $dist
if ($IncludeD3dCompiler -eq 'true') {
    # Redistribution is governed by the Windows SDK licence, not ANGLE's. Shipping it is the
    # default because the alternative is depending on whatever the end user's Windows happens
    # to have.
    Copy-Item -LiteralPath $d3dCompiler -Destination $dist
}
Copy-Item -LiteralPath (Join-Path $angle 'LICENSE') -Destination $dist

foreach ($headerDir in @('EGL', 'GLES2', 'GLES3', 'KHR')) {
    Copy-Item -LiteralPath (Join-Path $angle "include/$headerDir") `
              -Destination (Join-Path $dist 'include') -Recurse
}

# Import libraries, for anything that links ANGLE at build time rather than dlopen-ing it. Kept out
# of the zip root so they never reach a wheel.
foreach ($importLib in @('libEGL.dll.lib', 'libGLESv2.dll.lib')) {
    $path = Join-Path $built $importLib
    if (Test-Path -LiteralPath $path) { Copy-Item -LiteralPath $path -Destination (Join-Path $dist 'lib') }
}

# ---------------------------------------------------------------------------------------------
# 8. Provenance, zip, digest.
# ---------------------------------------------------------------------------------------------
$angleVersion = $null
$versionHeader = Join-Path $angle 'src/common/version.h'
if (Test-Path -LiteralPath $versionHeader) {
    $angleVersion = (Get-Content -LiteralPath $versionHeader) -join "`n"
}

$fileDigests = Get-ChildItem -LiteralPath $dist -Recurse -File | ForEach-Object {
    [PSCustomObject]@{
        path   = $_.FullName.Substring($dist.Length + 1).Replace('\', '/')
        bytes  = $_.Length
        sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash.ToLower()
    }
}

$provenance = [ordered]@{
    schema_version        = 1
    component             = 'angle'
    platform              = "windows-$TargetCpu"
    upstream              = 'https://chromium.googlesource.com/angle/angle'
    revision              = $Revision
    args_gn               = $argsGn
    ninja_targets         = @('libEGL', 'libGLESv2', 'copy_compiler_dll')
    d3dcompiler_47        = [ordered]@{
        shipped      = ($IncludeD3dCompiler -eq 'true')
        source       = 'Windows SDK Redist/D3D (copied by ANGLE''s copy_compiler_dll target)'
        file_version = $d3dVersion
        note         = 'Loaded by libGLESv2 at run time via LoadLibrary, with _46/_43 fallbacks.'
    }
    version_header        = $angleVersion
    files                 = $fileDigests
    built_at              = (Get-Date).ToUniversalTime().ToString('o')
    # A machine name, not a CI run URL: this build has no CI. Whoever ran it is the provenance.
    built_by              = "$env:COMPUTERNAME (manual run of scripts/angle/build_angle_windows.ps1)"
}
$provenance | ConvertTo-Json -Depth 6 |
    Set-Content -LiteralPath (Join-Path $dist 'PROVENANCE.json') -Encoding utf8

$zipName = "angle-$shortRevision-windows-$TargetCpu.zip"
$zipPath = Join-Path $OutDir $zipName
if (Test-Path -LiteralPath $zipPath) { Remove-Item -LiteralPath $zipPath -Force }
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::CreateFromDirectory(
    $dist, $zipPath, [System.IO.Compression.CompressionLevel]::Optimal, $false)

$digest = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLower()
$bytes = (Get-Item -LiteralPath $zipPath).Length
Set-Content -LiteralPath "$zipPath.sha256" -Value "$digest  $zipName" -Encoding ascii

Write-Host ''
Write-Host "zip:    $zipPath"
Write-Host "bytes:  $bytes"
Write-Host "sha256: $digest"

# ---------------------------------------------------------------------------------------------
# 9. What a human does next. Nothing publishes automatically, because nothing here runs in CI.
# ---------------------------------------------------------------------------------------------
Write-Host @"

Next, by hand:

  1. VERIFY before publishing (PROVENANCE-windows.md, "Verify before publishing"):
     the version string must read 'ANGLE 2.1.<count> git hash: $($Revision.Substring(0, 12))'
     where <count> is 'git rev-list HEAD --count' (28587 for be80ce59 -- a SHALLOW clone
     produces a WRONG count from a correct revision), and a self-test through these DLLs
     must reach a gl_probe with backend=d3d11.

  2. PUBLISH (travel-animator-shared is private; the release already exists if iOS/macOS
     published theirs at this revision):

       gh release upload angle-$shortRevision "$zipPath" ``
         --repo Lascade-Co/travel-animator-shared

  3. PIN. In Lascade-Co/actions, .github/workflows/publish-tada-wheel.yml:

       ANGLE_SHA256_WINDOWS_X64: $digest

     A digest is already pinned there from the 2026-08-11 build. Replacing it means every
     Windows wheel from now on carries THESE bytes, so only do so deliberately.

     If the zip was packed on Windows its entries may use backslash separators, which the
     ZIP spec forbids; repack with '/' before publishing. Only the root-level DLLs reach the
     wheel, so the fault is invisible there and shows up in include/ and lib/ instead.

  4. RECORD. Update PROVENANCE-windows.md with what this run produced and measured.
"@
