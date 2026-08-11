# ANGLE (Windows) — provenance

> ## ✅ STATUS: BUILT 2026-08-11. Not yet executed on a GPU.
>
> The recipe below has been run once, on a real Windows host, and it worked. What is **still**
> unverified is the *runtime*: nobody has yet created a GLES context through these DLLs, so
> `GlDriver.jvm.kt`'s `preloadWindowsGles` workaround remains untested and is the most likely
> first failure. See [Verify before publishing](#verify-before-publishing) — step 3 is the
> outstanding one.
>
> | | |
> |---|---|
> | Asset | `angle-be80ce59-windows-x64.zip`, 4,635,030 bytes, 24 entries |
> | SHA-256 | `8a2732e41e0e14471fe7f852c6ee13a1ace13ed2178231c8d16353e1b35a59cc` |
> | Version string | `ANGLE 2.1.28587 git hash: be80ce591a48` — **matches the pin exactly** |
> | Build host | Windows 10 Home Single Language · VS 2022 Build Tools 17.14.37 · MSVC 14.44.35207 · Windows SDK 10.0.26100.0 |
>
> **The asset was repacked before publication.** The Windows build wrote its zip entries with
> **backslash** separators (`include\EGL\egl.h`), which the ZIP spec forbids; on Linux and macOS
> `zipfile` treats those as filenames, so `include/` and `lib/` extract as a handful of flat files
> instead of a tree — silently. Only the separators were changed: all 24 entries are byte-identical
> to the original and every digest in the asset's own `PROVENANCE.json` still verifies against the
> repack. The digest above is the repacked one. `fetch_pinned_angle.py` now normalises backslashes
> defensively so a future build cannot reintroduce this quietly.
> (`PROVENANCE.json`'s internal `files` map still uses backslash keys. That is deliberate — it is
> the build's own record and was not rewritten; a consumer reading it should normalise.)
>
> ### Deviations from the pinned recipe — host toolchain only
>
> All four are recorded in the asset's `PROVENANCE.json`. None touches the ANGLE source, so the
> shader translator is identical to the macOS and iOS builds, which is what risk R14 turns on.
>
> - **Windows SDK 10.0.26100.0** instead of the upstream-pinned **10.0.28000.0** — 28000 ships only
>   with the VS 2026 Preview / Windows Insider SDK and was not obtainable via winget on this host.
>   `SDK_VERSION` patched in `build/vs_toolchain.py` and `build/toolchain/win/setup_toolchain.py`.
> - **`NTDDI_VERSION`** lowered from `NTDDI_WIN11_BR` (undefined in SDK 26100) to `NTDDI_WIN11_GE`
>   (`0x0A000010`) in `build/config/win/BUILD.gn`.
> - **`dbghelp.dll` made optional** in `_CopyDebugger` — Debugging Tools for Windows not installed;
>   those DLLs are a symbolization convenience and are not in the shipped payload.
> - **VS at a non-standard path** (`F:\VS\BuildTools`), located via `vs2022_install`.
>
> Two `gn` args were added beyond the block in [Build](#build): **`use_siso = false`** (the source
> tree was on `F:` and the SDK on `C:`; siso cannot relativize cross-drive header paths, plain ninja
> can) and **`angle_enable_wgpu = false`**.
>
> ⚠️ **This build is not guaranteed byte-identical to one produced against the pinned SDK 28000.**
> The revision matches, so pixels should; the toolchain does not, so binaries will not.

> **Doing the build?** [`BUILDING-WINDOWS.md`](BUILDING-WINDOWS.md) beside this file is the
> step-by-step operator guide — machine prep, the run, verification, publish, and a
> symptom→cause→fix table. This file stays the *record* of what is pinned and why.

The JVM host renderer needs GLES 3.0, and Windows has no system GLES. It therefore ships
[ANGLE](https://chromium.googlesource.com/angle/angle) on its D3D11 backend inside the wheel, the
same way macOS ships ANGLE on Metal.

**CI never builds this.** `scripts/angle/build_angle_windows.ps1` is run **by hand, once per
revision**, on a Windows box; the output is published as a SHA-256-pinned GitHub release asset and
every consumer downloads and verifies it. There is deliberately no workflow — a two-hour Windows
build reproducing a file that is already pinned by digest is the thing this arrangement exists to
avoid. (There *was* one, `.github/workflows/build-angle-windows.yml`, added in `94cc9b6` and
removed again; this document is where its contents went.)

## Upstream

| | |
|---|---|
| Repository | `https://chromium.googlesource.com/angle/angle` |
| Revision | `be80ce591a481c12d60c50d6040d40c035b40a2b` |
| Commit date | 2026-08-07 |
| Commit subject | `GL: Use refactored VAO availability function.` |
| Commit position (`git rev-list HEAD --count`) | `28587` |
| Expected version string | `ANGLE 2.1.28587 git hash: be80ce591a48` |
| License | BSD-3-Clause (ANGLE's `LICENSE`, shipped at the zip root) |

The same revision as the iOS xcframeworks (`travel-animator-shared`
`third_party/angle/PROVENANCE.md`) and the macOS dylibs (`PROVENANCE-macos.md` beside it). **One
revision everywhere is the entire point.** Windows previously took ANGLE 2.1.25511 out of Skiko's
Maven artifact while macOS took 2.1.28226 out of a Google Chrome install: two shader translators
and two rasterisations of the same frame, which is a golden-frame parity problem the moment both
platforms ship (risk R14). The revision is not a knob to turn casually — moving it means moving
iOS, macOS, Windows, `travel-animator-shared`'s `angleRevision`/`angleSha256`/`angleAssetUrl`, and
the `ANGLE_*` constants in `publish-tada-wheel.yml`, in one change.

`2.1.<N>` is just `git rev-list HEAD --count`, so a shallow clone produces a *wrong version string*
from a correct revision. Do not shallow-clone ANGLE.

## Builder requirements

| | |
|---|---|
| Host | Windows 10/11 x64 (or arm64, for an arm64 build) |
| Toolchain | Visual Studio 2022 with **Desktop development with C++** |
| SDK | Windows 10/11 SDK, **including the D3D redistributable** (`<sdk>/Redist/D3D/<cpu>/d3dcompiler_47.dll` must exist) |
| depot_tools | fetched by the script; unpinned on purpose (no release tags, self-updates on first use). It is the only unpinned input — ANGLE and everything `gclient sync` fetches are pinned by the revision's `DEPS`. |
| Disk | ~25 GB for checkout + depot_tools + build output |

Three environment facts a Chromium-family checkout needs on Windows, all set by the script:

```powershell
$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'   # use the installed VS, not Google's internal MSVC package
git config --global core.longpaths true
git config --global core.autocrlf false
```

`DEPOT_TOOLS_WIN_TOOLCHAIN` is the load-bearing one: left unset, `gn gen` runs
`build/vs_toolchain.py`, which tries to download Google's **internal** packaged MSVC toolchain from
a Google-only bucket and fails with an access error that reads like a network problem. Long paths
fail mid-`gclient sync` as "Filename too long" after twenty minutes of downloads; a CRLF-mangled
`.py`/`.gn` is a syntax error rather than a diff.

## Build

One `gn` output directory, `out/win-x64/args.gn`:

```gn
target_os = "win"
target_cpu = "x64"
is_debug = false
is_component_build = false

angle_enable_d3d11 = true
angle_enable_vulkan = false
angle_enable_gl = false
angle_enable_metal = false
angle_enable_null = false

symbol_level = 1
```

This is the macOS `args.gn` with the Metal backend swapped for D3D11. Two `gni/angle.gni` defaults
are why the "off" switches earn their lines:

- `angle_enable_swiftshader = angle_enable_vulkan && !is_android && is_clang` — leaving Vulkan on
  drags SwiftShader in: a whole second rasterizer, both build time and, if it were ever packaged, a
  second set of pixels.
- `angle_enable_hlsl = angle_enable_d3d11` — the HLSL translator the D3D11 backend needs comes
  along automatically. **Do not set `angle_enable_hlsl` here**; it is derived, and setting a derived
  arg is how the two drift.

`angle_enable_gl` is the WGL passthrough backend — a second driver path on the same machine, which
is the precise class of problem this build exists to retire. `angle_enable_null` is a no-op backend
for ANGLE's own test suite. Neither ships. (On arm64, `angle.gni` already forces `angle_enable_gl`
false via `is_win_arm64`; setting it false unconditionally keeps one args block for both CPUs.)

Build three targets, not two:

```powershell
autoninja -C out/win-x64 libEGL libGLESv2 copy_compiler_dll
```

`libEGL` / `libGLESv2` are still the root `BUILD.gn` target names at this revision, and
`angle_libs_suffix` is `""` off Android, so the outputs really are `libEGL.dll` / `libGLESv2.dll`.

## `d3dcompiler_47.dll` (risk R15)

**It is not compiled from ANGLE's source.** ANGLE's root `BUILD.gn` declares, at this revision:

```gn
_use_copy_compiler_dll = angle_has_build && is_win

copy("copy_compiler_dll") {
  sources = [ "$windows_sdk_path/Redist/D3D/$target_cpu/d3dcompiler_47.dll" ]
  outputs = [ "$root_out_dir/{{source_file_part}}" ]
}
```

i.e. the build **copies it out of the installed Windows SDK's redistributable D3D folder**, and
hangs that copy off `libANGLE_no_vulkan` as a `data_deps`. `data_deps` do reach ninja through the
linked target, so `autoninja libGLESv2` alone would very probably produce it — but "very probably"
is how R15 came to be an open risk in the first place. Naming the target makes the DLL a build
*product with a build failure attached*, instead of a file that either shows up or does not. The
script additionally asserts the file exists before packaging, and the failure message says what an
absent file means: the builder's Windows SDK has no D3D redistributable component (or the target
was renamed upstream).

Why it matters at run time: ANGLE's `libGLESv2.dll` resolves the HLSL compiler **by name at run
time** — `d3dcompiler_47.dll`, then `_46`, then `_43`, via `LoadLibrary`. It is neither a static nor
a delay-load import. This was confirmed by parsing the PE import and delay-import tables of Skiko's
shipped 2.1.25511 `libGLESv2.dll`. So a missing copy does not fail to load — it fails to compile a
shader, much later, inside a render.

Shipping it is a **licensing** decision, not a build one, which is why it is a script parameter
(`-IncludeD3dCompiler`) and not a constant. It sits in the SDK's `Redist` tree precisely because
that tree is the redistributable one; the default is to ship it, because the alternative is
depending on whatever the end user's Windows happens to have.

## Packaging

The zip **root** holds exactly what `build_jvm_payload.py --angle-dir` copies into a wheel's flat
`_jvm/angle/` — it iterates top-level *files* only, so `include/` and `lib/` are available to a
native consumer without reaching the wheel:

```
libEGL.dll
libGLESv2.dll
d3dcompiler_47.dll        (when -IncludeD3dCompiler true)
LICENSE                   ANGLE's, BSD-3-Clause: the notice travels with the binary
PROVENANCE.json           written by the script: args.gn, targets, per-file digests
include/{EGL,GLES2,GLES3,KHR}/
lib/{libEGL.dll.lib,libGLESv2.dll.lib}      import libs, kept OUT of the wheel
```

**`libEGL.dll` and `libGLESv2.dll` must stay in the same directory.** ANGLE's `libEGL` resolves
`libGLESv2` **by bare name** via `LoadLibraryW` out of its own module directory (risk R11, confirmed
by PE analysis, mitigation never executed). A split payload does not fail cleanly. This is the same
constraint the macOS build documents, for the same reason, with a nastier symptom there (`SIGSEGV`
at `pc=0x0`).

Asset name: `angle-<short-rev>-windows-<cpu>.zip`, e.g. **`angle-be80ce59-windows-x64.zip`** — the
name `publish-tada-wheel.yml` derives from `ANGLE_RELEASE_TAG` and the leg's `angle_platform`.

## Recipe

```powershell
# On a Windows box with VS 2022 + the Windows SDK. ~25 GB free.
git clone https://github.com/Lascade-Co/actions
.\actions\scripts\angle\build_angle_windows.ps1 `
  -Revision be80ce591a481c12d60c50d6040d40c035b40a2b `
  -TargetCpu x64 `
  -IncludeD3dCompiler true `
  -OutDir "$PWD\angle-out"
```

The script does everything the deleted workflow did — depot_tools, the checkout, `gn`, `ninja`, the
assertions, the staging, `PROVENANCE.json`, the zip and its SHA-256 — and prints the digest at the
end. Doing it by hand instead is the same sequence:

```powershell
$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'
git config --global core.longpaths true
git config --global core.autocrlf false

git clone https://chromium.googlesource.com/chromium/tools/depot_tools.git
$env:PATH = "$PWD\depot_tools;$env:PATH"
gclient --version                      # bootstraps depot_tools' own python/ninja/gn

git clone https://chromium.googlesource.com/angle/angle    # NOT --depth 1
cd angle
git checkout --detach be80ce591a481c12d60c50d6040d40c035b40a2b
python3 scripts\bootstrap.py
gclient sync --no-history -D

mkdir out\win-x64
# ...write the args.gn block above...
gn gen out/win-x64
autoninja -C out/win-x64 libEGL libGLESv2 copy_compiler_dll
```

### Likely first-run failures, in the order they would show up

1. depot_tools' toolchain probe — see `DEPOT_TOOLS_WIN_TOOLCHAIN` above.
2. Disk exhaustion during `gclient sync`.
3. The Windows SDK's `Redist/D3D` component being absent, caught by the `copy_compiler_dll`
   assertion rather than at render time.
4. `gn` rejecting an arg name that moved upstream — every name here was read at `be80ce59`, so this
   should only happen if the revision moves.

## Verify before publishing

Nothing below has been done; it is the checklist the first build owes, mirroring what the macOS
build actually did.

1. `libEGL.dll`, `libGLESv2.dll` and `d3dcompiler_47.dll` exist in `out/win-x64` — the script
   asserts this.
2. The version string reads `ANGLE 2.1.28587 git hash: be80ce591a48`. A different number means a
   shallow clone or a different revision, not a harmless variation.
3. A GLES 3.0 probe through the built pair: `java -jar ta-render.jar self-test --angle-dir <dist>`;
   expect exit 0 and a `gl_probe` event with
   `"driver":"bundled ANGLE at <dist>, backend=d3d11, os=Windows"`.

   **`ta-render.jar` must be BUILT ON THE WINDOWS BOX** — it is not portable, despite appearances.
   Verified by listing a macOS-built jar: LWJGL natives are fat (all six platforms, including
   `windows/x64/org/lwjgl/lwjgl.dll`), but Skiko natives are host-only —
   `libskiko-macos-{arm64,x64}.dylib` and no `skiko-windows-x64.dll`, because
   `shared/build.gradle.kts:278` derives `skiko-awt-runtime-$skikoHostOs-$skikoHostArch` from the
   *build host's* `os.name`. A copied jar loads, then dies at Skiko's first draw. The per-platform
   wheel CI is unaffected (each leg builds its own jar); only hand-staging is. See
   [`BUILDING-WINDOWS.md` §3.2](BUILDING-WINDOWS.md#32-the-gles-30-probe--you-must-build-ta-renderjar-on-this-machine).
4. A full render compared against the reference. macOS's build produced output **bit-identical** to
   the previous reference render; Windows will not match macOS byte-for-byte (different rasterizer),
   so the comparison that matters there is against the same-platform reference, or a perceptual one.
   Whatever is chosen, record the numbers **in this file** — this section is the deliverable of the
   first run.

## Publish

The release already exists at `angle-be80ce59` (the iOS asset created it), so this is an upload,
not a create:

```powershell
$env:GH_TOKEN = "<token with repo scope on Lascade-Co/travel-animator-shared>"

gh release upload angle-be80ce59 angle-out\angle-be80ce59-windows-x64.zip `
  --repo Lascade-Co/travel-animator-shared

# ...or, if that release does not exist yet:
gh release create angle-be80ce59 angle-out\angle-be80ce59-windows-x64.zip `
  --repo Lascade-Co/travel-animator-shared `
  --title "ANGLE be80ce59" `
  --notes "GLES 3.0. One revision per release; one asset per platform."

# The digest to pin, and the asset id for anything that resolves by id:
Get-FileHash angle-out\angle-be80ce59-windows-x64.zip -Algorithm SHA256
gh api repos/Lascade-Co/travel-animator-shared/releases/tags/angle-be80ce59 `
  --jq '.assets[] | {name, id, size}'
```

`travel-animator-shared` is **private**, so the browser-facing
`github.com/.../releases/download/...` URL answers 404 to an unauthenticated client. Consumers must
use the REST asset endpoint with `Accept: application/octet-stream` and a bearer token — which is
what `scripts/angle/fetch_pinned_angle.py` and shared's `downloadAngle` task both do.

Then, in the same change:

- set **`ANGLE_SHA256_WINDOWS_X64`** in `.github/workflows/publish-tada-wheel.yml` to the digest
  above (it is empty today, which is what fails the leg);
- replace the STATUS banner at the top of this file with what was built, measured and verified.

## The Windows gap — closed 2026-08-11

`ANGLE_SHA256_WINDOWS_X64` is now set, so the `windows-x64` leg fetches and verifies the published
asset like every other platform. What follows is the state it was in before, kept because it
records *why* the leg failed loudly rather than falling back — and why it must go on doing so if
the asset ever becomes unavailable.

Before the upload, `publish-tada-wheel.yml`'s `windows-x64` leg failed at **Fetch the pinned
ANGLE**, before it built anything, with a message naming this file. Deliberately:

- There is no Windows wheel to ship without an ANGLE pair — `build_jvm_payload.py` hard-fails a
  Windows payload with no `--angle-dir`, because the wheel would install and then die at
  `GlContext.createOffscreen`.
- The available fallback is *worse than nothing*: Skiko's
  `org.jetbrains.skiko:skiko-awt-runtime-angle-windows-x64` is ANGLE **2.1.25511**, a different
  shader translator from the 2.1.28587 every other platform runs (risk R14). It was wired up in an
  earlier pass and has been removed, along with `build_jvm_payload.py`'s `--angle-jar` door, so the
  skew cannot be reintroduced by accident.

A loud "Windows ANGLE asset not published yet" is the honest state. A silently different ANGLE is
not.
