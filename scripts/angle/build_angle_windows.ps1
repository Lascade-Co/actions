<#
.SYNOPSIS
    Build ANGLE's libEGL/libGLESv2 for Windows at a pinned revision and package them the way
    travel-animator-shared's `third_party/angle/PROVENANCE.md` packages the iOS slices.

.DESCRIPTION
    RUN THIS BY HAND, ONCE PER REVISION, ON A WINDOWS BOX. It is deliberately NOT wired to a
    workflow: CI never builds ANGLE, it downloads a SHA-256-pinned release asset (see
    `.github/workflows/publish-tada-wheel.yml`). A two-hour Windows runner reproducing a file
    that is already pinned by digest buys nothing. `.github/workflows/build-angle-windows.yml`
    existed briefly and was removed; the prose that was in it now lives in PROVENANCE-windows.md
    beside this file, which is the document to read first -- it carries the toolchain
    requirements, the d3dcompiler_47.dll finding, the verification checklist and the publish
    commands.

    This is the Windows counterpart of the recipe in that PROVENANCE file. Same upstream, same
    revision, same "publish one SHA-256-pinned release asset" ending -- only the `gn` args and the
    packaging differ, because Windows produces two plain DLLs rather than two xcframeworks.

    WHY IT EXISTS. Windows used to get its ANGLE from Skiko's published Maven artifact
    (`org.jetbrains.skiko:skiko-awt-runtime-angle-windows-x64`), which is ANGLE 2.1.25511, while
    macOS took 2.1.28226 out of a Google Chrome install. Two ANGLE builds means two shader
    translators and two rasterisations of the same frame, which is a golden-frame parity problem
    the moment both platforms ship (risk R14). macOS is now built from the pinned revision
    (2.1.28587) and Windows is the last platform still missing its own build -- which is why the
    windows-x64 wheel leg fails until this script has been run and its output published. Building
    every platform from ONE pinned revision is the entire point of this script; the revision is
    not a knob to turn casually.

    ⚠️ THIS SCRIPT HAS NEVER BEEN EXECUTED. Chromium's Windows build needs a Windows host with
    Visual Studio, and cross-compiling it from macOS is unsupported, so nothing here has run
    anywhere. Every `gn` argument, target name and output path below was read out of ANGLE's own
    `gni/angle.gni` and `BUILD.gn` AT THE PINNED REVISION rather than recalled, and the places
    where that reading could still be wrong are called out in the comments. Treat the first run as
    a debugging session, not a build.

.PARAMETER Revision
    ANGLE revision to build. Must be the same 40-hex revision every other platform is built from.

.PARAMETER TargetCpu
    `x64` or `arm64`. Only `x64` has a corresponding wheel today.

.PARAMETER IncludeD3dCompiler
    'true' to ship `d3dcompiler_47.dll` alongside the ANGLE pair, 'false' to build it, assert it,
    and then leave it out. See the "d3dcompiler_47.dll" section below -- this is a licensing
    decision, not a technical one, which is why it is a parameter and not a constant.

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

    # Deliberately a string, not [bool] and not [switch]. `-IncludeD3dCompiler false` bound to a
    # [bool] parameter arrives as the non-empty string "false" and converts to $true, which is the
    # single most common way a GitHub-Actions-to-PowerShell boundary silently inverts a flag.
    [ValidateSet('true', 'false')]
    [string]$IncludeD3dCompiler = 'true',

    [string]$WorkDir = $(if ($env:RUNNER_TEMP) { Join-Path $env:RUNNER_TEMP 'angle-build' } else { Join-Path $PWD 'angle-build' }),

    [string]$OutDir = $(Join-Path $PWD 'angle-out')
)

$ErrorActionPreference = 'Stop'

# PowerShell 7.4 made `$ErrorActionPreference = 'Stop'` apply to NATIVE commands too, so a non-zero
# `gn`/`ninja` would throw a generic "Program failed" before `Invoke-Native` could say which step
# died and with what arguments. Turn that back off and keep the explicit exit-code checks -- they
# produce the better message, and they behave the same on Windows PowerShell 5.1, where this
# variable does not exist and assigning it is inert.
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
# DEPOT_TOOLS_WIN_TOOLCHAIN=0 is the load-bearing one. Left unset, `gn gen` runs
# `build/vs_toolchain.py`, which tries to download Google's INTERNAL packaged MSVC toolchain from
# a Google-only bucket; on a GitHub runner that fails with an access error that reads like a
# network problem. `0` means "use the Visual Studio already installed on this machine", which on
# `windows-latest` is VS 2022 with the Windows SDK.
$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'

# Chromium-family checkouts contain paths past Windows' 260-character limit. Without this, the
# failure lands somewhere in the middle of `gclient sync` as "Filename too long", after twenty
# minutes of downloads.
Invoke-Native 'git: allow long paths' 'git' @('config', '--global', 'core.longpaths', 'true')

# A CRLF-mangled .py or .gn file is a syntax error, not a diff.
Invoke-Native 'git: disable autocrlf' 'git' @('config', '--global', 'core.autocrlf', 'false')

# ---------------------------------------------------------------------------------------------
# 2. depot_tools.
# ---------------------------------------------------------------------------------------------
#
# Not pinned, on purpose: depot_tools has no release tags and self-updates on first use anyway, so
# a pin here would be decoration. It is also the only unpinned input to this build -- ANGLE itself
# and everything `gclient sync` fetches are pinned by the revision's DEPS.
$depotTools = Join-Path $WorkDir 'depot_tools'
if (-not (Test-Path -LiteralPath $depotTools)) {
    Invoke-Native 'clone depot_tools' 'git' @(
        'clone', '--depth', '1',
        'https://chromium.googlesource.com/chromium/tools/depot_tools.git',
        $depotTools
    )
}
# PREPENDED. depot_tools ships its own python3/ninja/gn wrappers and they must win over the
# runner's; the `.bat` entry points are the supported ones on Windows, so they are named
# explicitly below rather than relied on via PATHEXT.
$env:PATH = "$depotTools;$env:PATH"

# First run bootstraps depot_tools' own Python and tooling. Doing it here keeps several minutes of
# bootstrap output out of the middle of the sync log.
Invoke-Native 'bootstrap depot_tools' (Join-Path $depotTools 'gclient.bat') @('--version')

# ---------------------------------------------------------------------------------------------
# 3. ANGLE at the pinned revision.
# ---------------------------------------------------------------------------------------------
#
# A full clone, matching PROVENANCE.md's recipe. A shallow one is tempting on a runner, but
# `gclient sync` resolves DEPS against the checked-out commit and a `--depth 1` fetch of an
# arbitrary revision is exactly the case that breaks; the clone is a few minutes, the debugging
# would not be.
$angle = Join-Path $WorkDir 'angle'
if (-not (Test-Path -LiteralPath $angle)) {
    Invoke-Native 'clone ANGLE' 'git' @(
        'clone', 'https://chromium.googlesource.com/angle/angle', $angle
    )
}
Push-Location $angle
try {
    Invoke-Native 'checkout the pinned revision' 'git' @('checkout', '--detach', $Revision)

    # Writes ANGLE's `.gclient` solution. Same two commands as PROVENANCE.md's step 1, except for
    # which python runs them: depot_tools is FIRST on PATH and ships `python3.bat`, while the
    # GitHub runner's own interpreter is `python` and there is no guarantee a bare `python3`
    # resolves to anything at all on Windows. Prefer depot_tools' -- it is the one every other
    # tool in this checkout is about to use -- and fall back rather than assume.
    $python = Join-Path $depotTools 'python3.bat'
    if (-not (Test-Path -LiteralPath $python)) { $python = 'python' }
    Invoke-Native 'bootstrap the gclient solution' $python @('scripts/bootstrap.py')
    Invoke-Native 'gclient sync' (Join-Path $depotTools 'gclient.bat') @('sync', '--no-history', '-D')

    # -----------------------------------------------------------------------------------------
    # 4. gn args -- the Windows/D3D11 counterpart of PROVENANCE.md's iOS/Metal block.
    # -----------------------------------------------------------------------------------------
    #
    # Every name here was read out of `gni/angle.gni` at this revision. Two of its defaults matter
    # and are the reason the "off" switches are worth their lines:
    #
    #   angle_enable_swiftshader = angle_enable_vulkan && !is_android && is_clang
    #       -- leaving Vulkan on drags SwiftShader into the build: a whole second rasterizer,
    #          which is both build time and, if it ever got packaged, a second set of pixels.
    #   angle_enable_hlsl = angle_enable_d3d11
    #       -- so the HLSL shader translator the D3D11 backend needs comes along automatically.
    #          Do NOT "add" angle_enable_hlsl here; it is derived, and setting a derived arg is
    #          how the two drift.
    #
    # angle_enable_gl is the WGL passthrough backend -- a second driver path on the same machine,
    # which is the precise class of problem this build exists to retire. angle_enable_null is a
    # no-op backend for ANGLE's own test suite. Neither ships.
    #
    # (On arm64, `angle.gni` already forces angle_enable_gl false via `is_win_arm64`. Setting it
    # false unconditionally keeps one args block for both CPUs.)
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
    # 5. Build -- and name `copy_compiler_dll` explicitly. (Risk R15.)
    # -----------------------------------------------------------------------------------------
    #
    # `d3dcompiler_47.dll` is NOT compiled from ANGLE's source. ANGLE's root BUILD.gn declares, at
    # this revision:
    #
    #     _use_copy_compiler_dll = angle_has_build && is_win
    #     copy("copy_compiler_dll") {
    #       sources = [ "$windows_sdk_path/Redist/D3D/$target_cpu/d3dcompiler_47.dll" ]
    #       outputs = [ "$root_out_dir/{{source_file_part}}" ]
    #     }
    #
    # i.e. the build COPIES it out of the installed Windows SDK's redistributable D3D folder, and
    # hangs that copy off `libANGLE_no_vulkan` as a `data_deps`. data_deps do reach ninja through
    # the linked target, so `autoninja libGLESv2` alone would very probably produce it -- but
    # "very probably" is how R15 came to be an open risk in the first place. Naming the target
    # makes the DLL a build PRODUCT with a build failure attached, instead of a file that either
    # shows up or does not.
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

# The d3dcompiler assertion runs whether or not we ship the file. Knowing the SDK component is
# present is worth a line either way: its absence means the Redist/D3D folder is missing from the
# runner image, which would silently change what a future `include_d3dcompiler=true` run produces.
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
# The zip's ROOT holds exactly what `build_jvm_payload.py --angle-dir` should copy into the
# wheel's flat `_jvm/angle/`: it iterates top-level FILES only, so `include/` and `lib/` below are
# available to a native consumer without reaching the wheel. ANGLE's LICENSE is at the root on
# purpose -- BSD-3-Clause wants the notice to travel with the binary, and at the root it rides
# into the wheel next to the DLLs it covers.
$dist = Join-Path $WorkDir "angle-dist-win-$TargetCpu"
New-CleanDirectory $dist
New-Item -ItemType Directory -Force -Path (Join-Path $dist 'include') | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dist 'lib') | Out-Null

Copy-Item -LiteralPath (Join-Path $built 'libEGL.dll')    -Destination $dist
Copy-Item -LiteralPath (Join-Path $built 'libGLESv2.dll') -Destination $dist
if ($IncludeD3dCompiler -eq 'true') {
    # Redistributing this file is governed by the Windows SDK licence, not by ANGLE's -- it sits
    # in the SDK's `Redist` tree precisely because that tree is the redistributable one. Shipping
    # it is the default because the alternative is depending on whatever the end user's Windows
    # happens to have; flipping the parameter is how that decision gets revisited without editing
    # this script.
    Copy-Item -LiteralPath $d3dCompiler -Destination $dist
}
Copy-Item -LiteralPath (Join-Path $angle 'LICENSE') -Destination $dist

foreach ($headerDir in @('EGL', 'GLES2', 'GLES3', 'KHR')) {
    Copy-Item -LiteralPath (Join-Path $angle "include/$headerDir") `
              -Destination (Join-Path $dist 'include') -Recurse
}

# Import libraries, for anything that links ANGLE at build time rather than dlopen-ing it. Kept
# out of the zip root so they never reach a wheel: 200 KB of dead weight per platform.
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
# 9. What a human does next.
# ---------------------------------------------------------------------------------------------
#
# Nothing publishes automatically, because nothing here runs in CI. The two consumers of this
# artifact are named explicitly so neither is forgotten: the release asset is what the wheel
# matrix downloads, and the digest is what it refuses to proceed without.
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

     It is empty today, and that emptiness is what fails the windows-x64 wheel leg with a
     message pointing at PROVENANCE-windows.md. Setting it is what unblocks Windows wheels.

  4. RECORD. Replace the STATUS banner at the top of PROVENANCE-windows.md with what this
     run actually produced and measured.
"@
