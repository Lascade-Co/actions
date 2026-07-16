#!/usr/bin/env bash
# Delete every version of a GHCR container package except the freshly-pushed current one.
# Env: OWNER (org login), PACKAGE (container name), CURRENT_DIGEST, GH_TOKEN.
set -euo pipefail

: "${OWNER:?}" "${PACKAGE:?}" "${CURRENT_DIGEST:?}" "${GH_TOKEN:?}"

versions_api="/orgs/$OWNER/packages/container/$PACKAGE/versions"

versions_file="$(mktemp)"
current_is_tagged=false
for _ in {1..10}; do
  gh api --paginate "$versions_api?per_page=100" \
    --jq '.[] | [.id, .name, ((.metadata.container.tags // []) | join(","))] | @tsv' \
    > "$versions_file"

  while IFS=$'\t' read -r _id digest tags; do
    if [[ "$digest" == "$CURRENT_DIGEST" && ",$tags," == *",latest,"* ]]; then
      current_is_tagged=true
      break
    fi
  done < "$versions_file"

  if [[ "$current_is_tagged" == true ]]; then
    break
  fi
  sleep 3
done

if [[ "$current_is_tagged" != true ]]; then
  echo "current digest is not yet visible with the latest tag; refusing cleanup" >&2
  exit 1
fi

while IFS= read -r id; do
  [[ -z "$id" ]] && continue
  echo "Deleting previous package version $id"
  gh api --method DELETE "$versions_api/$id"
done < <(
  awk -F '\t' -v current="$CURRENT_DIGEST" \
    '$2 != current { print $1 }' \
    "$versions_file"
)

only_current_remains=false
for _ in {1..10}; do
  gh api --paginate "$versions_api?per_page=100" \
    --jq '.[] | [.id, .name, ((.metadata.container.tags // []) | join(","))] | @tsv' \
    > "$versions_file"

  version_count="$(wc -l < "$versions_file" | tr -d ' ')"
  if [[ "$version_count" == "1" ]]; then
    IFS=$'\t' read -r _id digest tags < "$versions_file"
    if [[ "$digest" == "$CURRENT_DIGEST" && "$tags" == "latest" ]]; then
      only_current_remains=true
      break
    fi
  fi
  sleep 3
done

if [[ "$only_current_remains" != true ]]; then
  echo "package cleanup did not leave exactly the current latest version:" >&2
  cat "$versions_file" >&2
  exit 1
fi
