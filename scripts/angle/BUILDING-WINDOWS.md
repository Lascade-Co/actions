# Building ANGLE for Windows — operator guide

> **Read [`PROVENANCE-windows.md`](PROVENANCE-windows.md) first.** That file is the *record*: what
> revision, which `gn` args and why, the `d3dcompiler_47.dll` finding, and what must be updated
> when the build is published. This file is the *walkthrough*: what to do, in order, on a bare
> Windows box, and what to do when a step fails.
>
> ⚠️ **This build has never been executed.** Every argument, target name and path here was read out
> of ANGLE's own `gni/angle.gni` and `BUILD.gn` at the pinned revision, not recalled from a run.
> Budget a day, treat the first attempt as a debugging session, and write down what actually
> happened — see [Report back](#report-back).

---

## 0. What you are producing

One zip, published once per ANGLE revision, that every Windows consumer downloads and verifies by
SHA-256:

```
angle-be80ce59-windows-x64.zip
  libEGL.dll
  libGLESv2.dll
  d3dcompiler_47.dll
  LICENSE
  PROVENANCE.json
  include/{EGL,GLES2,GLES3,KHR}/
  lib/{libEGL.dll.lib,libGLESv2.dll.lib}
```

CI never builds this. It downloads it. A two-hour Windows build reproducing a file already pinned
by digest is exactly what this arrangement avoids.

**Time budget:** ~30 min setup, 40–90 min `gclient sync` (network-bound), 20–60 min compile.
**Disk:** 25 GB free, and check that before you start — exhaustion mid-sync is failure mode #2.

---

## 1. Prepare the machine

### 1.1 Visual Studio 2022

Install the **Desktop development with C++** workload. Via the GUI, or:

```powershell
winget install --id Microsoft.VisualStudio.2022.Community `
  --override "--add Microsoft.VisualStudio.Workload.NativeDesktop --includeRecommended --passive"
```

Community edition is fine. Build Tools alone (no IDE) also works — Chromium's `vs_toolchain.py`
finds it through the standard registry/`vswhere` lookup either way.

### 1.2 Windows SDK, **including the D3D redistributable**

The `--includeRecommended` above pulls a Windows SDK. What matters is not the version but that one
specific folder exists, because ANGLE's build copies `d3dcompiler_47.dll` straight out of it.
**Verify it directly rather than trusting a checkbox:**

```powershell
Get-ChildItem "C:\Program Files (x86)\Windows Kits\10\Redist\D3D" -Recurse -Filter d3dcompiler_47.dll
```

You want a hit under `...\Redist\D3D\x64\d3dcompiler_47.dll`. If nothing comes back, rerun the
Visual Studio Installer, **Modify → Individual components**, and add the latest *Windows 11 SDK*
(the full SDK component, not just "Windows Universal CRT"). This is failure mode #3 and it is far
better caught now than sixty minutes into a build.

### 1.3 Git, configured for a Chromium checkout

```powershell
winget install --id Git.Git
git config --global core.longpaths true
git config --global core.autocrlf false
```

Both settings are load-bearing. Without `core.longpaths`, `gclient sync` dies with "Filename too
long" *after* twenty-plus minutes of downloading. Without `autocrlf false`, a mangled `.py` or
`.gn` is a syntax error rather than a readable diff.

### 1.4 The one environment variable that decides whether this works

```powershell
$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'
```

Left unset, `gn gen` runs `build/vs_toolchain.py`, which tries to download Google's **internal**
packaged MSVC toolchain from a Google-only bucket and fails with an access error that reads like a
network problem. Set it in the shell you build from. The script sets it for you; if you build by
hand, set it yourself.

---

## 2. Build

### 2.1 The script (preferred)

```powershell
git clone https://github.com/Lascade-Co/actions
.\actions\scripts\angle\build_angle_windows.ps1 `
  -Revision be80ce591a481c12d60c50d6040d40c035b40a2b `
  -TargetCpu x64 `
  -IncludeD3dCompiler true `
  -OutDir "$PWD\angle-out"
```

Parameter notes:

| | |
|---|---|
| `-Revision` | 40-hex, validated by pattern. Must match every other platform — see [§3 of the provenance](PROVENANCE-windows.md#upstream). |
| `-TargetCpu` | `x64` or `arm64`. Only `x64` has a wheel today. |
| `-IncludeD3dCompiler` | A **string** `'true'`/`'false'`, deliberately not a `[bool]` or `[switch]`: `-IncludeD3dCompiler false` bound to a `[bool]` arrives as the non-empty string `"false"` and converts to `$true`. That inversion is the classic CI-to-PowerShell boundary bug. |
| `-WorkDir` | Scratch for depot_tools + checkout, ~25 GB. Defaults under `$PWD`. |

Every external call goes through `Invoke-Native`, which checks `$LASTEXITCODE` and names the step —
PowerShell does not fail a script on a non-zero native exit, and the `pwsh` shell only propagates
the *last* command's code.

### 2.2 By hand (same sequence, when you need to poke at a step)

```powershell
$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'

git clone https://chromium.googlesource.com/chromium/tools/depot_tools.git
$env:PATH = "$PWD\depot_tools;$env:PATH"
gclient --version                    # bootstraps depot_tools' own python/ninja/gn

git clone https://chromium.googlesource.com/angle/angle      # NOT --depth 1
cd angle
git checkout --detach be80ce591a481c12d60c50d6040d40c035b40a2b
python3 scripts\bootstrap.py
gclient sync --no-history -D

mkdir out\win-x64
```

Write `out\win-x64\args.gn`:

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

Then:

```powershell
gn gen out/win-x64
autoninja -C out/win-x64 libEGL libGLESv2 copy_compiler_dll
```

**Do not shallow-clone ANGLE.** The version string `2.1.<N>` is literally
`git rev-list HEAD --count`, so a shallow clone yields a *wrong version string from a correct
revision* — the worst kind of wrong, because everything still builds.

**Do not add `angle_enable_hlsl`.** It is derived (`angle_enable_hlsl = angle_enable_d3d11`);
setting a derived arg is how the two drift apart.

**Three ninja targets, not two.** `copy_compiler_dll` would *probably* come along as a `data_deps`
of `libGLESv2` — but "probably" is how this became an open risk. Naming it makes a missing
`d3dcompiler_47.dll` a build failure instead of a file that either shows up or doesn't. It matters
because `libGLESv2.dll` resolves the HLSL compiler **by name at run time** via `LoadLibrary`
(neither a static nor a delay-load import — confirmed by parsing the PE import tables of Skiko's
shipped 2.1.25511 build). A missing copy therefore does not fail to load; it fails to compile a
shader, much later, inside a render.

---

## 3. Verify before publishing

### 3.1 The version string

```powershell
Select-String -Path out\win-x64\gen\angle\id\commit.h -Pattern 'ANGLE_VERSION_STRING|ANGLE_COMMIT_HASH'
```

Expect `ANGLE 2.1.28587 git hash: be80ce591a48`. A different number means a shallow clone or a
different revision — not a harmless variation.

### 3.2 The GLES 3.0 probe — **you must build `ta-render.jar` on this machine**

> ⚠️ **Correction to the provenance checklist.** Its step 3 says to "stage `ta-render.jar` + a JRE +
> the DLLs". That understates the problem: **the jar is not portable.** Verified by listing a
> macOS-built jar — it carries LWJGL natives for all six platforms (`windows/x64/org/lwjgl/lwjgl.dll`
> and friends are present), but Skiko natives for the **build host only**:
>
> ```
> libskiko-macos-arm64.dylib      21 MB
> libskiko-macos-x64.dylib        22 MB
> (no skiko-windows-x64.dll, no libskiko-linux-x64.so)
> ```
>
> `shared/build.gradle.kts:278` declares
> `org.jetbrains.skiko:skiko-awt-runtime-$skikoHostOs-$skikoHostArch`, derived from the *build
> host's* `os.name`. So a macOS-built jar looks portable and dies at Skiko's first draw on Windows.
> The per-platform wheel CI is correct because each leg builds its own jar; a hand-copied jar is
> not.

So, on the Windows box, with **JDK 21**:

```powershell
git clone --recurse-submodules https://github.com/Lascade-Co/tada
cd tada
.\gradlew.bat :host:taJars
java -jar host\build\libs\ta-render.jar self-test --angle-dir <path-to-unzipped-dist>
```

Expect exit 0 and a `gl_probe` event on stderr reading:

```
"driver":"bundled ANGLE at <dist>, backend=d3d11, os=Windows"
```

If `:host:taJars` is more than you want on a build box, the alternative is to run the ANGLE pair
through any GLES 3.0 smoke test you trust — but then say so in the report, because the self-test is
what proves *our* loader path (see 3.3).

### 3.3 The thing most likely to be wrong

`libEGL.dll` resolves `libGLESv2.dll` **by bare name** via `LoadLibraryW`, out of its own module
directory. LWJGL loads `EGL_LIBRARY_NAME` by *absolute path*, which does **not** add that directory
to the dependent-DLL search path. `GlDriver.jvm.kt`'s `preloadWindowsGles` works around this by
`System.load`-ing libGLESv2 by absolute path first, so Windows' loader matches it by base name when
libEGL asks.

**That mitigation has never run.** It is marked UNVERIFIED in the source. If the self-test fails
inside `eglGetPlatformDisplayEXT` or with a DLL-not-found for `libGLESv2`, this is your suspect,
and the finding belongs in the report either way. Keep the two DLLs in the same directory
regardless — a split payload does not fail cleanly.

### 3.4 A full render

Compare against the same-platform reference. Windows will **not** match macOS byte-for-byte — a
different rasterizer — so a bit-identical comparison is the wrong test here, unlike macOS, where
the ANGLE build did produce output bit-identical to the previous reference. Use a perceptual
metric, and **record the numbers in `PROVENANCE-windows.md`**; that section is the deliverable of
the first run.

---

## 4. Publish

The release already exists (the iOS asset created it), so this is an upload:

```powershell
$env:GH_TOKEN = "<token with repo scope on Lascade-Co/travel-animator-shared>"

gh release upload angle-be80ce59 angle-out\angle-be80ce59-windows-x64.zip `
  --repo Lascade-Co/travel-animator-shared

Get-FileHash angle-out\angle-be80ce59-windows-x64.zip -Algorithm SHA256
```

Then, **in the same change**:

1. Set `ANGLE_SHA256_WINDOWS_X64` in `.github/workflows/publish-tada-wheel.yml` to that digest. It
   is empty today, which is what deliberately fails the `windows-x64` leg.
2. Replace the STATUS banner at the top of `PROVENANCE-windows.md` with what was built and measured.

`travel-animator-shared` is **private**, so the browser `releases/download/...` URL 404s for an
unauthenticated client. Consumers must use the REST asset endpoint with
`Accept: application/octet-stream` and a bearer token — which is what `fetch_pinned_angle.py` and
shared's `downloadAngle` task already do.

---

## 5. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `gn gen` fails with an access/permission error mentioning a Google bucket | `DEPOT_TOOLS_WIN_TOOLCHAIN` unset — it is trying to fetch Google's internal MSVC package | `$env:DEPOT_TOOLS_WIN_TOOLCHAIN = '0'` in the build shell |
| `gclient sync` dies with "Filename too long" after ~20 min | `core.longpaths` not set | `git config --global core.longpaths true`, delete the checkout, re-sync |
| `gclient sync` fails near the end, no clear error | Disk exhaustion | Free space to 25 GB+; the checkout is not resumable in practice |
| Build succeeds, `d3dcompiler_47.dll` missing | Windows SDK has no `Redist/D3D` component | §1.2. The script asserts this rather than shipping a silent gap |
| Syntax errors in `.py`/`.gn` files you never edited | CRLF mangling | `git config --global core.autocrlf false`, re-clone |
| `gn` rejects an arg name | The revision moved | Every name here was read at `be80ce59`; re-read `gni/angle.gni` at the new revision |
| Version string is not `2.1.28587` | Shallow clone | Re-clone without `--depth`; `2.1.<N>` is `git rev-list HEAD --count` |
| `self-test` fails loading `libGLESv2` | The bare-name `LoadLibraryW` resolution, §3.3 | Both DLLs in one directory; investigate `preloadWindowsGles` |
| `self-test` throws `LibraryLoadException` from Skiko | Jar built on a different OS | §3.2 — build the jar on Windows |

---

## 6. arm64

`-TargetCpu arm64` produces `angle-<rev>-windows-arm64.zip`. Two differences:

- `angle.gni` already forces `angle_enable_gl = false` via `is_win_arm64`; the args block above sets
  it false unconditionally so one block covers both CPUs.
- `d3dcompiler_47.dll` comes from `Redist\D3D\arm64\`, so §1.2's check must find *that* path.

There is **no arm64 Windows wheel leg today**, so this is only worth building when one is added.

---

## 7. Report back

The first run is the only chance to convert this from a recipe into a record. Capture:

- Wall-clock for sync and compile, and peak disk
- Every step that failed and what fixed it (especially anything in §5 that was wrong)
- The self-test `gl_probe` line verbatim
- Whether `preloadWindowsGles` was needed (§3.3) — this is the single most valuable unknown
- Render comparison numbers

Then edit `PROVENANCE-windows.md`: drop the STATUS banner, fill in the verification section, and
correct anything here that turned out to be wrong. **A recipe that has been run once and corrected
is worth more than one that reads well.**
