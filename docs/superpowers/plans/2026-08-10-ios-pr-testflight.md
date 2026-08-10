# iOS PR builds to TestFlight — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pull request opened in `Lascade-Co/travel-animator-ios` automatically produces a TestFlight build and reports its status back on the PR.

**Architecture:** No new central workflow. The existing `repository_dispatch` runner `.github/workflows/ios-build-debug.yml` learns to accept an optional `pr` field in its payload — checking out `refs/pull/N/head` instead of a branch, computing a PR-tagged build number, and posting a sticky comment on the PR. A thin trigger workflow in the app repo turns `pull_request` events into that dispatch. Two supporting changes: `scripts/ios/install_apple_cert.sh` derives the team and bundle identifiers from the provisioning profile it already decodes, and the five iOS signing secrets are copied from Infisical `prod` to `staging`.

**Tech Stack:** GitHub Actions (YAML), bash, `xcodebuild`, `xcrun altool`, Infisical CLI, `actionlint`, `shellcheck`.

**Spec:** [`docs/superpowers/specs/2026-08-10-ios-pr-testflight-design.md`](../specs/2026-08-10-ios-pr-testflight-design.md)
**Decisions:** [ADR-0008](../../adr/0008-ios-build-number-scheme.md) (build number), [ADR-0009](../../adr/0009-testflight-builds-are-release-with-debug-on.md) (build configuration)
**Vocabulary:** [`CONTEXT.md`](../../../CONTEXT.md) → *Language — iOS Builds*

## Global Constraints

Every task's requirements implicitly include this section.

- **Never rename, move, or delete `.github/workflows/ios-build-debug.yml`.** `github.run_number` feeds the build number and resets with the file. This is why the misleading `debug-ios` event name is kept. (ADR-0008)
- **Never change the `debug-ios` event type name.** The existing `debug.yml` trigger in the app repo dispatches to it.
- Use the latest versions of GitHub Actions (project `CLAUDE.md`). The versions already in this file are current: `actions/checkout@v6`, `actions/cache@v5`, `actions/upload-artifact@v7`, `actions/download-artifact@v8`, `actions/github-script@v8`, `actions/create-github-app-token@v3`, `peter-evans/create-or-update-comment@v5`, `peter-evans/repository-dispatch@v3`, `Infisical/secrets-action@v1.0.15`.
- Inline bash in a workflow only when under 10 lines; anything longer goes in `scripts/<domain>/` and is invoked by raw GitHub URL (project `CLAUDE.md`).
- Infisical: project ID `d3963969-c940-4fd7-84c9-7a38edaf404d`, secret path `/Build`, domain `https://secrets.lascade.com` (already the CLI's logged-in domain).
- Build number: `$(( 1000 + github.run_number ))`, with `.${pr}` appended when the payload carries a PR.
- Archive: `-configuration Release` plus `SWIFT_ACTIVE_COMPILATION_CONDITIONS='DEBUG $(inherited)'` and `GCC_PREPROCESSOR_DEFINITIONS='DEBUG=1 $(inherited)'`. Single quotes are required — `$(inherited)` must reach `xcodebuild` literally, not be run as a command substitution by bash.
- PR comment marker: `<!-- central-ios-testflight -->`.
- Never print a secret value to the terminal or to a workflow log. Compare lengths, not contents.

**Known-good values** for `travel-animator-ios`, verified against the real profile in Infisical on 2026-08-10 — use these as expected values in test steps:

| Thing | Value |
|---|---|
| Team ID | `ZG6QASWQ43` |
| Bundle ID | `com.travelanimator.routemap` |
| App profile `Name` | `main-app` |
| App profile `UUID` | `38d3d2f6-0ed5-4049-bf3e-9b4063b9edb7` |
| Marketing version | `3.9.3` |
| Train floor (existing build) | `213` |

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `scripts/ios/install_apple_cert.sh` (modify) | Also derive and export `*_PROFILE_TEAM_ID` / `*_PROFILE_BUNDLE_ID` | 1 |
| Infisical `travelanimator`/`staging`/`/Build` (data) | Hold the five `IOS_*` signing secrets | 2 |
| `.github/workflows/ios-build-debug.yml` (modify) | Accept a PR payload; build mechanics | 3 |
| `.github/workflows/ios-build-debug.yml` (modify) | PR status reporting | 4 |
| `travel-animator-ios/.github/workflows/pr-testflight.yml` (create, branch `ci`) | Turn `pull_request` into a dispatch | 5 |
| `docs/handoff/ios-testflight-missing-secrets.md` (create) | Delegate `SERVICE_PLIST_BASE64` and two pre-existing breaks | 6 |

Tasks 3 and 4 touch the same file but are separable: a reviewer can accept the build mechanics and reject the reporting, or vice versa. Do them in order.

**A note on testing this plan.** Workflow YAML has no unit test. Verification here is: `actionlint` / `shellcheck` as the static gate, local execution of each self-contained bash snippet against real inputs, and one end-to-end run (Task 7). Where a step can genuinely be tested first, it is. Where it cannot, the plan says so rather than inventing test theater.

---

### Task 1: Derive team and bundle identifiers from the provisioning profile

Removes two of the three missing Infisical keys and guarantees the values match the profile actually doing the signing.

**Files:**
- Modify: `scripts/ios/install_apple_cert.sh:61-73` (the `install_profile` function body)
- Test: run locally on macOS against the real profile — no test file, this is a shell script with no harness in this repo

**Interfaces:**
- Consumes: nothing from earlier tasks
- Produces: two new `GITHUB_ENV` variables per installed profile — `APP_PROFILE_TEAM_ID` (string, e.g. `ZG6QASWQ43`) and `APP_PROFILE_BUNDLE_ID` (string, e.g. `com.travelanimator.routemap`). `NSE_PROFILE_TEAM_ID` / `NSE_PROFILE_BUNDLE_ID` also appear and are unused. Task 3 reads `APP_PROFILE_TEAM_ID` and `APP_PROFILE_BUNDLE_ID`.

- [ ] **Step 1: Write the failing check**

Create `/tmp/check_derive.sh` (scratch, not committed):

```bash
#!/usr/bin/env bash
# Exercises install_profile's extraction against the real App Store profile.
set -euo pipefail

PID=d3963969-c940-4fd7-84c9-7a38edaf404d
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

export RUNNER_TEMP="$WORK"
export GITHUB_ENV="$WORK/github_env"
: > "$GITHUB_ENV"

IOS_PROVISION_PROFILE_BASE64=$(infisical secrets get IOS_PROVISION_PROFILE_BASE64 \
  --projectId "$PID" --env prod --path /Build --plain --silent)
export IOS_PROVISION_PROFILE_BASE64

# Only the profile-installation half of the script is under test, so stub the
# certificate half: run install_profile in isolation by sourcing nothing and
# reimplementing the caller contract the script provides.
cd "$WORK"
mkdir -p "$HOME/Library/MobileDevice/Provisioning Profiles" \
         "$HOME/Library/Developer/Xcode/UserData/Provisioning Profiles"

bash -c '
  source /dev/stdin <<< "$(sed -n "/^install_profile()/,/^}/p" '"$OLDPWD"'/scripts/ios/install_apple_cert.sh)"
  PROFILE_DIR_LEGACY="$HOME/Library/MobileDevice/Provisioning Profiles"
  PROFILE_DIR_NEW="$HOME/Library/Developer/Xcode/UserData/Provisioning Profiles"
  install_profile "$IOS_PROVISION_PROFILE_BASE64" APP required
'

echo "--- GITHUB_ENV ---"
cat "$GITHUB_ENV"

grep -q '^APP_PROFILE_TEAM_ID=ZG6QASWQ43$'                  "$GITHUB_ENV" || { echo "FAIL: team id"; exit 1; }
grep -q '^APP_PROFILE_BUNDLE_ID=com.travelanimator.routemap$' "$GITHUB_ENV" || { echo "FAIL: bundle id"; exit 1; }
echo "PASS"
```

Note: `$OLDPWD` inside the nested `bash -c` resolves to the actions repo checkout because of the `cd "$WORK"` above it. Run the script from the repo root.

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /Users/rohittp/Data/Lascade/actions && bash /tmp/check_derive.sh
```

Expected: prints the current `GITHUB_ENV` containing only `APP_PROFILE_UUID` and `APP_PROFILE_NAME`, then `FAIL: team id`, exit 1.

- [ ] **Step 3: Implement the derivation**

In `scripts/ios/install_apple_cert.sh`, replace the body of `install_profile` from the `local uuid name` declaration through the two `echo ... >> "$GITHUB_ENV"` lines with:

```bash
  local plist uuid name team app_id bundle
  plist=$(security cms -D -i "$file")
  uuid=$(plutil -extract UUID raw - <<<"$plist")
  name=$(plutil -extract Name raw - <<<"$plist")
  team=$(plutil -extract TeamIdentifier.0 raw - <<<"$plist")
  app_id=$(plutil -extract Entitlements.application-identifier raw - <<<"$plist")
  bundle="${app_id#"$team".}"

  cp "$file" "$PROFILE_DIR_LEGACY/$uuid.mobileprovision"
  cp "$file" "$PROFILE_DIR_NEW/$uuid.mobileprovision"

  echo "${prefix}_PROFILE_UUID=$uuid" >> "$GITHUB_ENV"
  echo "${prefix}_PROFILE_NAME=$name" >> "$GITHUB_ENV"
  echo "${prefix}_PROFILE_TEAM_ID=$team" >> "$GITHUB_ENV"

  # A wildcard profile yields "*" or "com.example.*" — useless as a bundle id,
  # so leave it unset and let the workflow fall back to IOS_BUNDLE_ID.
  case "$bundle" in
    *\*) echo "Skipping ${prefix}_PROFILE_BUNDLE_ID (wildcard profile: $app_id)" ;;
    *)   echo "${prefix}_PROFILE_BUNDLE_ID=$bundle" >> "$GITHUB_ENV" ;;
  esac
```

Also update the "Outputs" comment block at the top of the file (lines 18-22) to list the two new variables:

```bash
# Outputs (written to GITHUB_ENV, only for profiles that were installed):
#   APP_PROFILE_UUID,    APP_PROFILE_NAME,    APP_PROFILE_TEAM_ID,    APP_PROFILE_BUNDLE_ID
#   NSE_PROFILE_UUID,    NSE_PROFILE_NAME,    NSE_PROFILE_TEAM_ID,    NSE_PROFILE_BUNDLE_ID
#   WIDGET_PROFILE_UUID, WIDGET_PROFILE_NAME, WIDGET_PROFILE_TEAM_ID, WIDGET_PROFILE_BUNDLE_ID
#   WATCH_PROFILE_UUID,  WATCH_PROFILE_NAME,  WATCH_PROFILE_TEAM_ID,  WATCH_PROFILE_BUNDLE_ID
# _PROFILE_BUNDLE_ID is omitted for wildcard profiles.
```

- [ ] **Step 4: Run the check to verify it passes**

```bash
cd /Users/rohittp/Data/Lascade/actions && bash /tmp/check_derive.sh
```

Expected: `GITHUB_ENV` now contains four `APP_PROFILE_*` lines, then `PASS`.

- [ ] **Step 5: Lint**

```bash
shellcheck scripts/ios/install_apple_cert.sh
```

Expected: no output (exit 0). If `SC2086` fires on a pre-existing line, leave it — do not fix unrelated warnings in this task.

- [ ] **Step 6: Commit**

```bash
git add scripts/ios/install_apple_cert.sh
git commit -m "feat(ios): export team and bundle ids derived from the provisioning profile"
```

---

### Task 2: Copy the five iOS signing secrets from Infisical prod to staging

`ios-build-debug.yml` moves to `env-slug: staging` in Task 3; these must be there first or the build fails at certificate installation.

**Files:**
- Modify: Infisical data only. No files in the repo change.

**Interfaces:**
- Consumes: nothing
- Produces: `IOS_CERTIFICATE_BASE64`, `IOS_CERTIFICATE_PASSWORD`, `IOS_PROVISION_PROFILE_BASE64`, `IOS_NSE_PROVISION_PROFILE_BASE64`, `IOS_SHARE_EXT_PROVISION_PROFILE_BASE64` readable at `travelanimator`/`staging`/`/Build`. Task 3 depends on these.

- [ ] **Step 1: Record the current state**

```bash
PID=d3963969-c940-4fd7-84c9-7a38edaf404d
for ENV in prod staging; do
  echo "=== $ENV ==="
  infisical secrets --projectId "$PID" --env "$ENV" --path /Build -o json --silent \
    | python3 -c "
import sys,json
d=json.load(sys.stdin); items = d if isinstance(d,list) else d.get('secrets',[])
for s in sorted(items, key=lambda x: x.get('secretKey','')):
    k=s.get('secretKey'); v=s.get('secretValue') or ''
    if k.startswith('IOS_'): print(f'  {k} len={len(v)}')
"
done
```

Expected before the copy — prod lists five `IOS_*` keys with lengths `4316`, `9`, `16804`, `18796`, `16876`; staging lists none.

- [ ] **Step 2: Copy the values**

Values are written from a file rather than a command-line argument so they never appear in `ps` output. Command substitution strips the trailing newline `--plain` adds — that matters for `IOS_CERTIFICATE_PASSWORD`, where a stray newline breaks `security import`.

```bash
set -euo pipefail
PID=d3963969-c940-4fd7-84c9-7a38edaf404d
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

for KEY in IOS_CERTIFICATE_BASE64 IOS_CERTIFICATE_PASSWORD \
           IOS_PROVISION_PROFILE_BASE64 IOS_NSE_PROVISION_PROFILE_BASE64 \
           IOS_SHARE_EXT_PROVISION_PROFILE_BASE64; do
  V=$(infisical secrets get "$KEY" --projectId "$PID" --env prod --path /Build --plain --silent)
  printf '%s' "$V" > "$TMP/$KEY"
  infisical secrets set "$KEY=@$TMP/$KEY" --projectId "$PID" --env staging --path /Build --silent >/dev/null
  echo "copied $KEY (${#V} chars)"
done
```

- [ ] **Step 3: Verify by length, not by value**

```bash
PID=d3963969-c940-4fd7-84c9-7a38edaf404d
python3 - <<'PY'
import json, subprocess
PID = "d3963969-c940-4fd7-84c9-7a38edaf404d"
def lens(env):
    out = subprocess.check_output(
        ["infisical","secrets","--projectId",PID,"--env",env,"--path","/Build","-o","json","--silent"],
        text=True)
    d = json.loads(out); items = d if isinstance(d,list) else d.get("secrets",[])
    return {s["secretKey"]: len(s.get("secretValue") or "")
            for s in items if s["secretKey"].startswith("IOS_")}
p, s = lens("prod"), lens("staging")
ok = True
for k, n in sorted(p.items()):
    match = s.get(k) == n
    ok &= match
    print(f"{'OK ' if match else 'BAD'} {k}: prod={n} staging={s.get(k)}")
print("PASS" if ok else "FAIL")
PY
```

Expected: five `OK` lines and `PASS`.

- [ ] **Step 4: Verify the copied certificate password actually works**

A length match would not catch a mangled value. Decode the staging certificate with the staging password:

```bash
set -euo pipefail
PID=d3963969-c940-4fd7-84c9-7a38edaf404d
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
infisical secrets get IOS_CERTIFICATE_BASE64 --projectId "$PID" --env staging --path /Build --plain --silent \
  | tr -d '\n\r ' | base64 --decode > "$TMP/cert.p12"
PW=$(infisical secrets get IOS_CERTIFICATE_PASSWORD --projectId "$PID" --env staging --path /Build --plain --silent)
openssl pkcs12 -in "$TMP/cert.p12" -passin "pass:$PW" -nokeys -legacy 2>/dev/null \
  | openssl x509 -noout -subject
```

Expected: a subject line naming Apple Distribution and the team. If `openssl` reports a MAC verify failure, the password copied with trailing whitespace — redo Step 2 for that key.

- [ ] **Step 5: No commit**

Nothing in the repo changed. Record completion in the task list only.

---

### Task 3: Teach `ios-build-debug.yml` to build a PR

Build mechanics only. Reporting is Task 4.

**Files:**
- Modify: `.github/workflows/ios-build-debug.yml`

**Interfaces:**
- Consumes: `APP_PROFILE_TEAM_ID`, `APP_PROFILE_BUNDLE_ID` from Task 1; the staging secrets from Task 2
- Produces: build job outputs `version` (string, marketing version, already exists), `app_name` (string, already exists), `build_number` (string, e.g. `1010.765`), `owner` (string, e.g. `Lascade-Co`), `name` (string, e.g. `travel-animator-ios`). Task 4 reads `build_number`, `version`, `app_name`, `owner`, `name`.

- [ ] **Step 1: Header — run name, permissions, concurrency**

Replace lines 2-14 with:

```yaml
run-name: "${{ github.event.client_payload.repo }} · ${{ github.event.client_payload.branch || format('PR #{0}', github.event.client_payload.pr) }}"

on:
  repository_dispatch:
    types: [debug-ios]

permissions:
  contents: read
  pull-requests: write
  issues: write

concurrency:
  group: debug-ios-${{ github.event.client_payload.repo }}-${{ github.event.client_payload.pr || github.event.client_payload.branch }}
  cancel-in-progress: true
```

- [ ] **Step 2: Build job header — timeout and outputs**

Replace the `build:` job header (lines 16-21) with:

```yaml
  build:
    runs-on: macos-latest
    timeout-minutes: 60
    outputs:
      version: ${{ steps.version.outputs.version }}
      app_name: ${{ steps.app_meta.outputs.app_name }}
      build_number: ${{ steps.build_number.outputs.build }}
      owner: ${{ steps.repo.outputs.owner }}
      name: ${{ steps.repo.outputs.name }}
```

- [ ] **Step 3: Choose the checkout ref**

Insert immediately after the `Mint GitHub App token` step and before `Checkout target repo @ branch`:

```yaml
      - name: Decide ref to checkout (PR HEAD or named branch)
        id: ref
        shell: bash
        run: |
          PR="${{ github.event.client_payload.pr }}"
          BR="${{ github.event.client_payload.branch }}"
          if [ -n "$PR" ] && [ "$PR" != "null" ]; then
            echo "ref=refs/pull/${PR}/head" >> "$GITHUB_OUTPUT"
          elif [ -n "$BR" ]; then
            echo "ref=refs/heads/${BR}" >> "$GITHUB_OUTPUT"
          else
            echo "No PR or branch in payload" >&2
            exit 1
          fi
```

Then change the checkout step's name and `ref`:

```yaml
      - name: Checkout target repo at PR HEAD/branch
        uses: actions/checkout@v6
        with:
          repository: ${{ github.event.client_payload.repo }}
          ref: ${{ steps.ref.outputs.ref }}
          token: ${{ steps.app-token.outputs.token }}
          fetch-depth: 0
```

- [ ] **Step 4: Switch Infisical to staging**

In the `Fetch build secrets from Infisical` step, change:

```yaml
          env-slug: "prod"
```

to:

```yaml
          env-slug: "staging"
```

- [ ] **Step 5: Resolve team and bundle identifiers**

Insert immediately after `Install additional provisioning profiles` and before `Switch Xcode project to manual signing`. Order matters — `fix_ios_signing.py` consumes the resolved team ID.

```yaml
      - name: Resolve team and bundle identifiers
        id: ids
        shell: bash
        run: |
          TEAM="${IOS_TEAM_ID:-${APP_PROFILE_TEAM_ID:-}}"
          BUNDLE="${IOS_BUNDLE_ID:-${APP_PROFILE_BUNDLE_ID:-}}"
          : "${TEAM:?no IOS_TEAM_ID in Infisical and none derivable from the profile}"
          : "${BUNDLE:?no IOS_BUNDLE_ID in Infisical and none derivable from the profile}"
          echo "Team $TEAM, bundle $BUNDLE"
          echo "team=$TEAM"     >> "$GITHUB_OUTPUT"
          echo "bundle=$BUNDLE" >> "$GITHUB_OUTPUT"
```

- [ ] **Step 6: Feed the resolved team id to the signing rewrite**

Replace the `Switch Xcode project to manual signing` step with:

```yaml
      - name: Switch Xcode project to manual signing
        env:
          PBXPROJ_PATH: ${{ steps.detect.outputs.project }}/project.pbxproj
          IOS_TEAM_ID: ${{ steps.ids.outputs.team }}
        run: curl -sSL https://raw.githubusercontent.com/Lascade-Co/actions/main/scripts/ios/fix_ios_signing.py | python3 -
```

- [ ] **Step 7: Use the resolved identifiers in ExportOptions**

In `Generate ExportOptions.plist`, replace every `${{ env.IOS_TEAM_ID }}` with `${{ steps.ids.outputs.team }}` and every `${{ env.IOS_BUNDLE_ID }}` with `${{ steps.ids.outputs.bundle }}`. The step becomes:

```yaml
      - name: Generate ExportOptions.plist
        shell: bash
        run: |
          /usr/libexec/PlistBuddy -c "Clear dict" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :method string app-store-connect" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :teamID string ${{ steps.ids.outputs.team }}" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :signingStyle string manual" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :uploadSymbols bool true" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :provisioningProfiles dict" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :provisioningProfiles:${{ steps.ids.outputs.bundle }} string ${{ env.APP_PROFILE_UUID }}" ExportOptions.plist
          /usr/libexec/PlistBuddy -c "Add :provisioningProfiles:${{ steps.ids.outputs.bundle }}.OneSignalNotificationServiceExtension string ${{ env.NSE_PROFILE_UUID }}" ExportOptions.plist
          if [ -n "${{ env.SHARE_EXT_PROFILE_UUID }}" ]; then
            /usr/libexec/PlistBuddy -c "Add :provisioningProfiles:${{ steps.ids.outputs.bundle }}.MapLinkShareExtension string ${{ env.SHARE_EXT_PROFILE_UUID }}" ExportOptions.plist
          fi
```

- [ ] **Step 8: Compute the build number**

Insert immediately before the `Archive` step:

```yaml
      # 1000 + run_number clears the 3.9.3 train's floor of ~213; run_number
      # supplies monotonicity. Renaming this workflow file resets run_number
      # and breaks uploads — see docs/adr/0008-ios-build-number-scheme.md.
      - name: Compute build number
        id: build_number
        shell: bash
        run: |
          BUILD=$(( 1000 + ${{ github.run_number }} ))
          PR="${{ github.event.client_payload.pr }}"
          if [ -n "$PR" ] && [ "$PR" != "null" ]; then BUILD="${BUILD}.${PR}"; fi
          echo "Build number $BUILD"
          echo "build=$BUILD" >> "$GITHUB_OUTPUT"
```

- [ ] **Step 9: Test the build-number snippet locally before trusting it**

The snippet is self-contained bash. Run both branches with the `${{ }}` values substituted by hand:

```bash
run_number=10
for PR in "765" "" "null"; do
  BUILD=$(( 1000 + run_number ))
  if [ -n "$PR" ] && [ "$PR" != "null" ]; then BUILD="${BUILD}.${PR}"; fi
  echo "pr='${PR}' -> ${BUILD}"
done
```

Expected exactly:

```
pr='765' -> 1010.765
pr='' -> 1010
pr='null' -> 1010
```

- [ ] **Step 10: Archive with the build number and the DEBUG flag**

Replace the `Archive` step with:

```yaml
      - name: Archive
        id: archive
        run: |
          xcodebuild archive \
            -project "${{ steps.detect.outputs.project }}" \
            -scheme "${{ steps.detect.outputs.scheme }}" \
            -configuration Release \
            -destination "generic/platform=iOS" \
            -archivePath build/App.xcarchive \
            -clonedSourcePackagesDirPath SourcePackages \
            -disableAutomaticPackageResolution \
            CURRENT_PROJECT_VERSION="${{ steps.build_number.outputs.build }}" \
            CODE_SIGN_STYLE=Manual \
            SWIFT_ACTIVE_COMPILATION_CONDITIONS='DEBUG $(inherited)' \
            GCC_PREPROCESSOR_DEFINITIONS='DEBUG=1 $(inherited)'
```

The single quotes are load-bearing: they stop bash from running `inherited` as a command. Release optimisation with `DEBUG` on is ADR-0009 — testers reach the `#if DEBUG` toggles without an `-Onone` binary.

- [ ] **Step 11: Add step ids the failure comment will read in Task 4**

Add `id: spm` to `Resolve SPM dependencies` and `id: export` to `Export IPA`. (`id: archive` was added in Step 10.) Add nothing else to those steps.

- [ ] **Step 12: Lint**

```bash
actionlint .github/workflows/ios-build-debug.yml
```

Expected: no output (exit 0).

- [ ] **Step 13: Commit**

```bash
git add .github/workflows/ios-build-debug.yml
git commit -m "feat(ios): build PR heads, tag the build number with the PR, compile with DEBUG on"
```

---

### Task 4: Report status back on the pull request

**Files:**
- Modify: `.github/workflows/ios-build-debug.yml`

**Interfaces:**
- Consumes: `steps.app-token.outputs.token` in the build job; build job outputs `build_number`, `version`, `app_name`, `owner`, `name` from Task 3
- Produces: `steps.status.outputs.comment-id` within the build job. Nothing later depends on it.

Every comment step is guarded on the payload carrying a PR, so the existing `workflow_dispatch` branch path is untouched.

- [ ] **Step 1: Purge stale comments and post "build started"**

Insert immediately after `Mint GitHub App token` and before `Decide ref to checkout`:

```yaml
      - name: Purge previous comments from this workflow
        if: ${{ github.event.client_payload.pr && github.event.client_payload.pr != '' && github.event.client_payload.pr != 'null' }}
        uses: actions/github-script@v8
        with:
          github-token: ${{ steps.app-token.outputs.token }}
          script: |
            const [owner, repo] = "${{ github.event.client_payload.repo }}".split("/");
            const issue_number = Number("${{ github.event.client_payload.pr }}");
            const marker = "<!-- central-ios-testflight -->";
            const comments = await github.paginate(
              github.rest.issues.listComments,
              { owner, repo, issue_number, per_page: 100 }
            );
            let deleted = 0;
            for (const c of comments) {
              if ((c.body || "").includes(marker)) {
                await github.rest.issues.deleteComment({ owner, repo, comment_id: c.id });
                deleted++;
              }
            }
            core.info(`Deleted ${deleted} prior central-ios-testflight comments`);

      - name: Comment on PR that the build started
        id: status
        if: ${{ github.event.client_payload.pr && github.event.client_payload.pr != '' && github.event.client_payload.pr != 'null' }}
        uses: peter-evans/create-or-update-comment@v5
        with:
          edit-mode: replace
          token: ${{ steps.app-token.outputs.token }}
          repository: ${{ github.event.client_payload.repo }}
          issue-number: ${{ github.event.client_payload.pr }}
          body: |
            <!-- central-ios-testflight -->
            🏗️ TestFlight build started.
            Watch progress [here](${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }})
```

- [ ] **Step 2: Comment when the build job fails**

Append as the last step of the `build` job:

```yaml
      - name: Comment on PR if the build failed
        if: ${{ failure() && github.event.client_payload.pr && github.event.client_payload.pr != '' && github.event.client_payload.pr != 'null' }}
        uses: actions/github-script@v8
        with:
          github-token: ${{ steps.app-token.outputs.token }}
          script: |
            const outcomes = {
              "SPM resolution": "${{ steps.spm.outcome }}",
              "Archive":        "${{ steps.archive.outcome }}",
              "Export IPA":     "${{ steps.export.outcome }}",
            };
            const stage = Object.keys(outcomes).find(k => outcomes[k] === "failure") || "Build";
            const [owner, repo] = "${{ github.event.client_payload.repo }}".split("/");
            const body = [
              "<!-- central-ios-testflight -->",
              `❌ **${stage}** failed.`,
              `[Check logs](${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }})`,
            ].join("\n");
            await github.rest.issues.updateComment({
              owner, repo,
              comment_id: Number("${{ steps.status.outputs.comment-id }}"),
              body,
            });
```

- [ ] **Step 3: Give the publish job an App token**

Insert as the first two steps of the `publish` job, before `Download IPA artifact`:

```yaml
      - name: Mint GitHub App token
        id: app-token
        if: ${{ github.event.client_payload.pr && github.event.client_payload.pr != '' && github.event.client_payload.pr != 'null' }}
        uses: actions/create-github-app-token@v3
        with:
          client-id: ${{ secrets.CI_APP_CLIENT_ID }}
          private-key: ${{ secrets.CI_APP_PRIVATE_KEY }}
          owner: ${{ needs.build.outputs.owner }}
          repositories: ${{ needs.build.outputs.name }}
```

- [ ] **Step 4: Comment on success**

Insert in the `publish` job immediately after `Upload to App Store Connect (TestFlight)` and before the Telegram steps:

```yaml
      - name: Comment on PR with the TestFlight build
        if: ${{ github.event.client_payload.pr && github.event.client_payload.pr != '' && github.event.client_payload.pr != 'null' }}
        uses: peter-evans/create-or-update-comment@v5
        with:
          token: ${{ steps.app-token.outputs.token }}
          repository: ${{ github.event.client_payload.repo }}
          issue-number: ${{ github.event.client_payload.pr }}
          edit-mode: replace
          body: |
            <!-- central-ios-testflight -->
            ✅ Uploaded to TestFlight — **${{ needs.build.outputs.app_name }}**,
            version `${{ needs.build.outputs.version }}`, build `${{ needs.build.outputs.build_number }}`.

            Find it in TestFlight under version ${{ needs.build.outputs.version }} → build ${{ needs.build.outputs.build_number }}.
            Processing takes a few minutes after this comment appears.

            Workflow run: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}
```

- [ ] **Step 5: Comment when the publish job fails**

Append as the last step of the `publish` job:

```yaml
      - name: Comment on PR if the upload failed
        if: ${{ failure() && github.event.client_payload.pr && github.event.client_payload.pr != '' && github.event.client_payload.pr != 'null' }}
        uses: peter-evans/create-or-update-comment@v5
        with:
          token: ${{ steps.app-token.outputs.token }}
          repository: ${{ github.event.client_payload.repo }}
          issue-number: ${{ github.event.client_payload.pr }}
          edit-mode: replace
          body: |
            <!-- central-ios-testflight -->
            ❌ **TestFlight upload** failed for build `${{ needs.build.outputs.build_number }}`.
            [Check logs](${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }})
```

- [ ] **Step 6: Add the PR to the Telegram messages**

In the `publish` job's two existing Telegram steps, add one line to each `MSG` heredoc, after the `Branch:` line:

```
          <b>PR:</b> ${{ github.event.client_payload.title || 'n/a' }}
```

and in the success message add the build number after the version line:

```
          <b>Build:</b> ${{ needs.build.outputs.build_number }}
```

Leave the `continue-on-error: true` on the failure notification exactly as it is.

- [ ] **Step 7: Lint**

```bash
actionlint .github/workflows/ios-build-debug.yml
```

Expected: no output (exit 0).

- [ ] **Step 8: Commit**

```bash
git add .github/workflows/ios-build-debug.yml
git commit -m "feat(ios): report TestFlight build status on the pull request"
```

---

### Task 5: The trigger in `travel-animator-ios`

**Files:**
- Create: `<worktree>/.github/workflows/pr-testflight.yml` on branch `ci`

**Interfaces:**
- Consumes: the `debug-ios` event contract from Tasks 3-4 — payload `{repo, pr, branch, title, project_slug}`
- Produces: nothing consumed by later tasks. Task 7 validates it end to end.

- [ ] **Step 1: Create the worktree**

The main checkout at `/Users/rohittp/Data/Lascade/travel-animator-ios` has an in-progress merge with unresolved conflicts (`UU CLAUDE.md`, `UU project.pbxproj`). Do not touch it — work in a separate worktree.

```bash
cd /Users/rohittp/Data/Lascade/travel-animator-ios
git fetch origin ci
git worktree add /Users/rohittp/Data/Lascade/travel-animator-ios-ci --track -b ci origin/ci
cd /Users/rohittp/Data/Lascade/travel-animator-ios-ci
git status --short && git log --oneline -1
```

Expected: a clean worktree at `f6b21a132 Merge pull request #765 ...`. If a local `ci` branch already exists, drop `-b ci --track` and use `git worktree add /Users/rohittp/Data/Lascade/travel-animator-ios-ci ci`.

- [ ] **Step 2: Write the trigger**

Create `/Users/rohittp/Data/Lascade/travel-animator-ios-ci/.github/workflows/pr-testflight.yml`:

```yaml
name: Request central TestFlight build

on:
  pull_request:
    types: [opened, reopened, ready_for_review]

jobs:
  dispatch:
    concurrency:
      group: central-ios-testflight-${{ github.event.pull_request.number }}
      cancel-in-progress: true

    # Drafts build when they are marked ready, not when they are opened.
    # Fork PRs have no access to CENTRAL_DISPATCH_TOKEN.
    if: >-
      github.event.pull_request.draft == false &&
      github.event.pull_request.head.repo.full_name == github.repository
    runs-on: ubuntu-latest
    steps:
      - name: Dispatch to central repo
        uses: peter-evans/repository-dispatch@v3
        with:
          token: ${{ secrets.CENTRAL_DISPATCH_TOKEN }}
          repository: Lascade-Co/actions
          event-type: debug-ios
          client-payload: >-
            {"repo":${{ toJSON(github.repository) }},
             "pr":${{ toJSON(github.event.pull_request.number) }},
             "branch":${{ toJSON(github.head_ref) }},
             "title":${{ toJSON(github.event.pull_request.title) }},
             "project_slug": "travelanimator"}
```

- [ ] **Step 3: Lint**

```bash
cd /Users/rohittp/Data/Lascade/travel-animator-ios-ci
actionlint .github/workflows/pr-testflight.yml
```

Expected: no output (exit 0).

- [ ] **Step 4: Confirm the payload is valid JSON**

`repository-dispatch` silently fails on malformed JSON. Render the template by hand and parse it:

```bash
python3 -c "
import json
p = '''{\"repo\":\"Lascade-Co/travel-animator-ios\",
 \"pr\":742,
 \"branch\":\"feat/x\",
 \"title\":\"A title with \\\"quotes\\\"\",
 \"project_slug\": \"travelanimator\"}'''
print(json.loads(p))
"
```

Expected: a dict with all five keys. This checks the shape; `toJSON` handles the escaping at runtime.

- [ ] **Step 5: Commit and push the branch**

```bash
cd /Users/rohittp/Data/Lascade/travel-animator-ios-ci
git add .github/workflows/pr-testflight.yml
git commit -m "ci: request a central TestFlight build when a PR is ready"
git push origin ci
```

---

### Task 6: Handoff document for the remaining secret

**Files:**
- Create: `docs/handoff/ios-testflight-missing-secrets.md` in the actions repo

**Interfaces:**
- Consumes: nothing
- Produces: nothing

- [ ] **Step 1: Write the document**

```markdown
# Handoff: iOS TestFlight — one missing secret and two pre-existing breaks

For an agent with browser access and the Infisical CLI, logged in to
`https://secrets.lascade.com` as a user with write access to the `travelanimator`
project.

Infisical project ID: `d3963969-c940-4fd7-84c9-7a38edaf404d`
Secret path: `/Build`

## 1. Blocking — `SERVICE_PLIST_BASE64`

`.github/workflows/ios-build-debug.yml` decodes this into `GoogleService-Info.plist`
before archiving. It does not exist in any environment of the `travelanimator`
project, so today the step writes a zero-byte plist and Firebase breaks at runtime.
The archive itself still succeeds, which is why this is invisible until a tester
opens the app.

**Source:** Firebase console → the Travel Animator project → Project settings →
Your apps → the iOS app with bundle id `com.travelanimator.routemap` → download
`GoogleService-Info.plist`.

**Set it in both environments** — `ios-build-release.yml` reads `prod`, and the
debug/PR runner reads `staging`:

```bash
PID=d3963969-c940-4fd7-84c9-7a38edaf404d
base64 -i GoogleService-Info.plist | tr -d '\n' > /tmp/plist.b64
for ENV in staging prod; do
  infisical secrets set "SERVICE_PLIST_BASE64=@/tmp/plist.b64" \
    --projectId "$PID" --env "$ENV" --path /Build
done
rm -f /tmp/plist.b64
```

**Verify** without printing the value:

```bash
for ENV in staging prod; do
  infisical secrets get SERVICE_PLIST_BASE64 --projectId "$PID" --env "$ENV" \
    --path /Build --plain --silent | base64 --decode \
    | plutil -extract BUNDLE_ID raw -
done
```

Expected: `com.travelanimator.routemap` twice.

## Not needed — `IOS_TEAM_ID` and `IOS_BUNDLE_ID`

These were previously missing too. They are now derived at build time from the App
Store provisioning profile by `scripts/ios/install_apple_cert.sh`
(`APP_PROFILE_TEAM_ID`, `APP_PROFILE_BUNDLE_ID`). Setting them in Infisical is
optional — the workflow prefers Infisical values when present and falls back to the
derived ones. Do not set them unless the profile is a wildcard, which it is not:
team `ZG6QASWQ43`, bundle `com.travelanimator.routemap`.

## 2. Pre-existing — `ios-build-release.yml` uploads a build number below the floor

App Store Connect requires the build number to strictly increase within a marketing
version. `travel-animator-ios` is at `3.9.3` with `CURRENT_PROJECT_VERSION = 213`, so
that train's floor is ~213. `ios-build-release.yml` uploads `github.run_number`,
currently ~9. Every release upload would be rejected.

Fixing it is not just an offset. PR builds now occupy `1000+run_number` dotted with
the PR number in the same train, and the two workflows have independent counters that
will cross. The release workflow needs a band that always sits above the PR builds —
see `docs/adr/0008-ios-build-number-scheme.md` for the full constraint.

## 3. Pre-existing — `increment_version.sh` does not exist

`ios-build-release.yml` runs `bash increment_version.sh` in the checked-out app repo
to bump the marketing version after a release. No such file exists in
`travel-animator-ios` on any branch. That step fails.
```

- [ ] **Step 2: Verify the verification command actually works**

The `plutil -extract BUNDLE_ID` check in the document must be correct before an agent relies on it. Confirm the key name against the plist you have access to — a `GoogleService-Info.plist` uses `BUNDLE_ID`, not `CFBundleIdentifier`. If the local repo has a committed copy:

```bash
find /Users/rohittp/Data/Lascade/travel-animator-ios-ci -name GoogleService-Info.plist -not -path '*/.build/*' \
  | head -1 | xargs -I{} plutil -extract BUNDLE_ID raw {}
```

Expected: `com.travelanimator.routemap`. If no committed copy exists, replace that verification block in the document with a length check and note that the key name is unverified.

- [ ] **Step 3: Commit**

```bash
cd /Users/rohittp/Data/Lascade/actions
git add docs/handoff/ios-testflight-missing-secrets.md
git commit -m "docs: handoff for the remaining iOS TestFlight secret and two pre-existing breaks"
```

---

### Task 7: End-to-end validation

**Requires explicit user go-ahead** — this pushes to `main` of the actions repo and opens a real pull request.

**Files:** none

- [ ] **Step 1: Push the actions repo changes**

`repository_dispatch` only ever runs the workflow from the default branch, so the runner changes must be on `main` before any dispatch can exercise them.

```bash
cd /Users/rohittp/Data/Lascade/actions
git log --oneline origin/main..HEAD
git push origin main
```

Expected: the four commits from Tasks 1, 3, 4 and 6.

- [ ] **Step 2: Open a throwaway PR from `ci`**

```bash
cd /Users/rohittp/Data/Lascade/travel-animator-ios-ci
gh pr create --base main --head ci \
  --title "ci: TestFlight on PR (validation, do not merge)" \
  --body "Validating the central TestFlight trigger. Close without merging."
```

`pull_request` resolves the workflow from the merge ref, so the trigger fires even though `main` does not have the file yet.

- [ ] **Step 3: Confirm the dispatch fired**

```bash
cd /Users/rohittp/Data/Lascade/travel-animator-ios-ci
gh run list --workflow=pr-testflight.yml --limit 3
```

Expected: one `success` run of "Request central TestFlight build".

- [ ] **Step 4: Watch the central run**

```bash
cd /Users/rohittp/Data/Lascade/actions
gh run list --workflow=ios-build-debug.yml --limit 1
gh run watch "$(gh run list --workflow=ios-build-debug.yml --limit 1 --json databaseId -q '.[0].databaseId')"
```

Check in the log:
- `Decide ref to checkout` resolved `refs/pull/<N>/head`
- `Resolve team and bundle identifiers` printed `Team ZG6QASWQ43, bundle com.travelanimator.routemap`
- `Compute build number` printed `1010.<N>` (first component will be `1000 + run_number` at the time)
- The PR carries a `🏗️ TestFlight build started` comment

- [ ] **Step 5: Record the outcome honestly**

The spec expects this run to reach `Archive`. Whether it gets past `Archive` is unknown — no iOS build in this repo has ever completed one, and diagnosing that is explicitly out of scope. Report what actually happened:

- Reached `Archive` and beyond → the wiring works; note whether the TestFlight upload succeeded and whether the success comment landed.
- Failed in `Archive` → the wiring works and the app build is broken. Capture the compiler error and hand it to whoever owns the iOS project. Confirm the PR got the `❌ Archive failed` comment — that is this task's real deliverable.
- Failed before `Archive` → a plan defect. Fix it in the relevant task and re-run.

- [ ] **Step 6: Close the throwaway PR**

```bash
cd /Users/rohittp/Data/Lascade/travel-animator-ios-ci
gh pr close ci --comment "Validation complete."
```

- [ ] **Step 7: Merge `ci` to `main` — only after the run is understood**

Until this lands on `main`, the trigger covers nothing: of the last 40 PRs in the repo, 38 targeted `main` and none targeted `ci`. Open this as a normal reviewed PR, not a direct push.

---

## Self-Review

**Spec coverage**

| Spec requirement | Task |
|---|---|
| Ref selection, PR head vs branch | 3 (Step 3) |
| Build configuration Release + DEBUG | 3 (Step 10) |
| Build number `1000+run.pr`, exposed as output | 3 (Steps 2, 8, 9) |
| Team/bundle derived from profile, wildcard guard | 1; 3 (Steps 5-7) |
| Infisical `env-slug: staging` | 3 (Step 4) |
| Concurrency keyed on pr-or-branch | 3 (Step 1) |
| `timeout-minutes: 60` | 3 (Step 2) |
| `run-name`, permissions | 3 (Step 1) |
| Purge + started + failure comment in `build` | 4 (Steps 1, 2) |
| App token + success + failure comment in `publish` | 4 (Steps 3, 4, 5) |
| Telegram gains PR title and build number | 4 (Step 6) |
| Accepted gap: no Telegram on build failure | Not implemented by design — stated in the spec |
| Trigger: events, draft guard, fork guard, payload | 5 (Step 2) |
| Infisical staging copy of five `IOS_*` keys | 2 |
| Handoff doc for `SERVICE_PLIST_BASE64` | 6 |
| Handoff notes the two pre-existing breaks | 6 (Step 1, sections 2 and 3) |
| Rollout: validate on `ci`, then `main` | 7 |

No gaps.

**Placeholder scan:** none. Every code step carries the literal content. Expected outputs are concrete values verified against the live profile and the live Infisical project on 2026-08-10.

**Type consistency:** `steps.ids.outputs.team` / `.bundle` (Task 3 Step 5) are consumed in Task 3 Steps 6 and 7 under the same names. `steps.build_number.outputs.build` (Step 8) is consumed in Step 10 and surfaced as the job output `build_number` (Step 2), read in Task 4 as `needs.build.outputs.build_number`. `steps.status.outputs.comment-id` (Task 4 Step 1) is read in Task 4 Step 2. `steps.spm` / `steps.archive` / `steps.export` ids are created in Task 3 Steps 10-11 and read in Task 4 Step 2. Build job outputs `owner` / `name` (Task 3 Step 2) are read in Task 4 Step 3. `APP_PROFILE_TEAM_ID` / `APP_PROFILE_BUNDLE_ID` are written in Task 1 and read in Task 3 Step 5 under identical names.

**Ordering constraints, all satisfied by task order:** Task 2 must precede any run of Task 3's workflow. Task 1 must precede Task 3 Step 5. Task 3 must precede Task 4 (Task 4 reads outputs Task 3 defines). Tasks 1-6 must all precede Task 7.
