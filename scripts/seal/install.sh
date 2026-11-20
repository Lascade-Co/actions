#!/usr/bin/env bash
# Install the sealing toolkit for one job: verified age, seal-bash/seal-bash-e,
# seal-pack, seal-unpack and the repo key's public half, all under $RUNNER_TEMP/seal.
# Two ways in:
# - this repo's verbatim "Seal job output" step: SEAL_REF=github.workflow_sha; the
#   tools and scripts/seal/repo-key.pub are downloaded from this repo at SEAL_REF.
# - the actions/seal composite (other repos): SEAL_TOOLKIT_DIR is the toolkit in the
#   action's own checkout; the key is .github/seal/repo-key.pub of SEAL_KEY_REPO at
#   commit SEAL_KEY_REF, read through the contents API with SEAL_TOKEN. There is no
#   fallback to this repo's key.
# Test-only knobs (refused when GITHUB_ACTIONS=true): SEAL_SOURCE_DIR,
# SEAL_PUBKEY_FILE, SEAL_NO_SUDO, SEAL_AGE_SHA256_OVERRIDE.
set -euo pipefail

: "${RUNNER_TEMP:?}" "${GITHUB_ENV:?}" "${GITHUB_PATH:?}" "${GITHUB_JOB:?}" "${GITHUB_RUN_ATTEMPT:?}"
AGE_VERSION=v1.3.2
TOOLS="seal-bash seal-pack seal-unpack"
seal="$RUNNER_TEMP/seal"
bin="$seal/bin"
stage="$seal/stage"

die() { rm -rf "$stage"; echo "seal: $*" >&2; exit 1; }

if [ "${GITHUB_ACTIONS:-}" = true ]; then
  for v in SEAL_SOURCE_DIR SEAL_PUBKEY_FILE SEAL_NO_SUDO SEAL_AGE_SHA256_OVERRIDE; do
    [ -z "${!v:-}" ] || die "$v is a test-only setting and is refused on GitHub Actions"
  done
fi
# A second install would restart the step numbering and overwrite sealed segments.
[ ! -e "$seal/artifact-name" ] || die "already installed in this job; run the 'Seal job output' step once"

# fetch <url> <dest>: bounded retries; promote only a successful, nonempty download.
fetch() {
  rm -f "$2.tmp"
  if curl -fsSL --retry 3 --retry-delay 2 --retry-all-errors --connect-timeout 10 --max-time 120 \
      -o "$2.tmp" "$1" && [ -s "$2.tmp" ]; then
    mv "$2.tmp" "$2"
  else
    rm -f "$2.tmp"
    die "download failed: $1"
  fi
}

# fetch_key <dest>: the caller repo's committed public key at SEAL_KEY_REF. No -f, so
# an HTTP error is final (only transport errors and transient statuses are retried)
# and gets its own message. The token goes to curl on stdin, never in argv or output.
fetch_key() {
  local code
  rm -f "$1.tmp"
  # shellcheck disable=SC2153 # SEAL_KEY_REF comes from the environment and is validated before any call
  code=$(printf 'Authorization: Bearer %s\n' "$SEAL_TOKEN" | curl -sSL --retry 3 --retry-delay 2 \
      --retry-all-errors --connect-timeout 10 --max-time 60 -H @- \
      -H 'Accept: application/vnd.github.raw' -H 'X-GitHub-Api-Version: 2022-11-28' \
      -w '%{http_code}' -o "$1.tmp" \
      "https://api.github.com/repos/$SEAL_KEY_REPO/contents/$CALLER_KEY?ref=$SEAL_KEY_REF") || true
  if [ "$code" = 200 ] && [ -s "$1.tmp" ]; then mv "$1.tmp" "$1"; return; fi
  rm -f "$1.tmp"
  case "$code" in
    200) die "$keyname is empty" ;;
    404) die "$keyname was not found (HTTP 404); commit the repo's age public key there and give the job contents: read" ;;
    401|403) die "cannot read $keyname (HTTP $code); the job's token needs contents: read" ;;
    *) die "could not download $keyname (HTTP ${code:-none})" ;;
  esac
}

case "$(uname -s) $(uname -m)" in
  'Linux x86_64')  plat=linux-amd64  sum=cbe24006683f8eb669266162894b9a522a1af52f2665fbc63a4bb032ed26ac10 ;;
  'Linux aarch64') plat=linux-arm64  sum=6b8dc4333c53a5a57c9e5834e3a48f92605d7154014cd07269ff3327db5d37f4 ;;
  'Darwin arm64')  plat=darwin-arm64 sum=e2020b073c44f692685a24d6abc378817eb81ffaaf49fd0531ef8565f767f2f5 ;;
  *) die "unsupported platform: $(uname -s) $(uname -m)" ;;
esac
sum="${SEAL_AGE_SHA256_OVERRIDE:-$sum}"

# Tools: the test source dir, the action's toolkit dir, or this repo at SEAL_REF.
src=${SEAL_SOURCE_DIR:-${SEAL_TOOLKIT_DIR:-}}
# Key: the test file, the caller repo, the test source dir, or this repo at SEAL_REF.
CALLER_KEY=.github/seal/repo-key.pub
keyname='repo-key.pub'
if [ -n "${SEAL_PUBKEY_FILE:-}" ]; then keysrc='file'
elif [ -n "${SEAL_KEY_REPO:-}" ]; then
  keysrc='api'
  keyname="$CALLER_KEY in ${SEAL_KEY_REPO} at ${SEAL_KEY_REF:-}"
  owner=${SEAL_KEY_REPO%%/*} name=${SEAL_KEY_REPO#*/}
  [[ "$SEAL_KEY_REPO" == */* && "$owner" =~ ^[A-Za-z0-9-]+$ && "$name" =~ ^[A-Za-z0-9_.-]+$ \
     && "$name" != . && "$name" != .. ]] \
    || die "SEAL_KEY_REPO must be owner/name, not '$SEAL_KEY_REPO'"
  [[ "${SEAL_KEY_REF:-}" =~ ^[0-9a-f]{40}$ ]] || die "SEAL_KEY_REF must be a full commit SHA to read $CALLER_KEY"
  [ -n "${SEAL_TOKEN:-}" ] || die "SEAL_TOKEN is required to read $CALLER_KEY from $SEAL_KEY_REPO"
elif [ -n "${SEAL_TOOLKIT_DIR:-}" ]; then
  die "SEAL_TOOLKIT_DIR needs SEAL_KEY_REPO: the key must come from $CALLER_KEY in the calling repo"
elif [ -n "${SEAL_SOURCE_DIR:-}" ]; then keysrc='source'
else keysrc='raw'; fi

if [ -z "$src" ] || [ "$keysrc" = raw ]; then
  [[ "${SEAL_REF:-}" =~ ^[0-9a-f]{40}$ ]] || die "SEAL_REF must be a full commit SHA"
  raw="https://raw.githubusercontent.com/Lascade-Co/actions/$SEAL_REF/scripts/seal"
fi

rm -rf "$stage"
mkdir -p "$stage" "$seal/log" "$seal/out" "$seal/art"

# Stage everything, verify it, and only then install into bin.
tgz="$stage/age.tar.gz"
fetch "https://github.com/FiloSottile/age/releases/download/$AGE_VERSION/age-$AGE_VERSION-$plat.tar.gz" "$tgz"
if command -v sha256sum >/dev/null; then got=$(sha256sum "$tgz"); else got=$(shasum -a 256 "$tgz"); fi
[ "${got%% *}" = "$sum" ] || die "age $AGE_VERSION checksum mismatch for $plat"
tar -xzf "$tgz" -C "$stage"

case $keysrc in
  file)   cp "$SEAL_PUBKEY_FILE" "$stage/repo-key.pub" ;;
  api)    fetch_key "$stage/repo-key.pub" ;;
  source) cp "$SEAL_SOURCE_DIR/repo-key.pub" "$stage/repo-key.pub" ;;
  raw)    fetch "$raw/repo-key.pub" "$stage/repo-key.pub" ;;
esac
recipient=$(cat "$stage/repo-key.pub")
[[ "$recipient" =~ ^age1[02-9ac-hj-np-z]{58}$ ]] || die "$keyname is not a single age recipient (one age1... line)"

for t in $TOOLS; do
  if [ -n "$src" ]; then cp "$src/$t" "$stage/$t" || die "toolkit file missing: $src/$t"
  else fetch "$raw/$t" "$stage/$t"; fi
done

mkdir -p "$bin"
printf '%s\n' "$recipient" >"$seal/repo-key.pub"
for t in age age-keygen; do install -m 0755 "$stage/age/$t" "$bin/$t"; done
for t in $TOOLS; do install -m 0755 "$stage/$t" "$bin/$t"; done
# One script, two shell modes: the basename picks the bash flags.
install -m 0755 "$stage/seal-bash" "$bin/seal-bash-e"
rm -rf "$stage"

rand=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
name="sealed-log-$GITHUB_JOB-$GITHUB_RUN_ATTEMPT-$rand"
printf '%s\n' "$name" >"$seal/artifact-name"
printf '0\n' >"$seal/seq"

python3 -c 'import json, sys; print(json.dumps(dict(zip(("job", "attempt", "runner_name", "os", "started_at"), sys.argv[1:]))))' \
  "$GITHUB_JOB" "$GITHUB_RUN_ATTEMPT" "${RUNNER_NAME:-}" "${RUNNER_OS:-}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  | "$bin/age" -R "$seal/repo-key.pub" -o "$seal/out/meta.json.age"

if [ "${SEAL_NO_SUDO:-}" != 1 ]; then
  # Fallback in case the runner resolves custom shells before GITHUB_PATH applies.
  sudo -n mkdir -p /usr/local/bin || true
  for t in seal-bash seal-bash-e seal-pack seal-unpack; do
    sudo -n ln -sf "$bin/$t" "/usr/local/bin/$t" || echo "::warning::seal: could not link $t into /usr/local/bin"
  done
fi

printf '%s\n' "$bin" >>"$GITHUB_PATH"
printf 'SEAL_LOG_ARTIFACT=%s\n' "$name" >>"$GITHUB_ENV"
repo=${GITHUB_REPOSITORY:-}
echo "🔒 Step output in this job is sealed. Open it at https://ci.lascade.com/?repo=${repo#*/}&run=${GITHUB_RUN_ID:-}&attempt=$GITHUB_RUN_ATTEMPT"
