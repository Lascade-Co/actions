# Stable TARS Runpod resources

TARS production uses one fixed Runpod Serverless endpoint, one fixed template,
and one fixed registry-auth record. Their IDs live in Infisical
`/deployment` as:

- `RUNPOD_ENDPOINT_ID`
- `RUNPOD_TEMPLATE_ID`
- `RUNPOD_REGISTRY_AUTH_ID`

The ordinary `main` deployment never creates or deletes Runpod resources. It
pauses the TARS dispatcher, drains the endpoint, records the exact prior image
and endpoint version, and updates only the fixed template's immutable image.
The endpoint remains configured for zero minimum and two maximum workers.

## Adopt the existing production resources once

Use `migrate` for an existing exact TARS v1 endpoint/template/auth chain. This
renames the template and endpoint in place and emits the same IDs; it does not
create or delete anything.

Prepare owner-only files containing the Runpod API key and only the currently
deployed endpoint ID. The API key comes from Infisical `/runtime`; the endpoint
ID comes from the live dispatcher before `/deployment` contains any of the
future stable keys. The migration resolves the endpoint's exact bound template
and that template's registry auth from verified Runpod inventory; the following
block validates and stores all three unchanged values in Infisical
`/deployment`, then restores the dispatcher:

```sh
(
set -eu
umask 077
work="$(mktemp -d)"
dispatcher_paused=0

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ "$dispatcher_paused" -eq 1 ]; then
    ssh -i ~/.ssh/deployment -o IdentitiesOnly=yes \
      ubuntu@"$DEPLOY_SSH_HOST" \
      docker service scale tars_dispatcher=1 >&2 || status=1
  fi
  rm -rf -- "$work"
  exit "$status"
}
trap cleanup EXIT HUP INT TERM

infisical secrets get RUNPOD_API_KEY \
  --domain https://secrets.lascade.com \
  --projectId a05e73f7-f45a-43af-8ddd-3d5b4f8bd8e5 \
  --env prod \
  --path /runtime \
  --plain \
  --silent > "$work/RUNPOD_API_KEY"

DEPLOY_SSH_HOST="$(
  infisical secrets get DEPLOY_SSH_HOST \
    --domain https://secrets.lascade.com \
    --projectId a05e73f7-f45a-43af-8ddd-3d5b4f8bd8e5 \
    --env prod \
    --path /deployment \
    --plain \
    --silent
)"
test -n "$DEPLOY_SSH_HOST"

ssh -i ~/.ssh/deployment -o IdentitiesOnly=yes \
  ubuntu@"$DEPLOY_SSH_HOST" docker service inspect \
  --format '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}' \
  tars_dispatcher |
  sed -n 's/^TARS_RUNPOD_ENDPOINT_ID=//p' > "$work/RUNPOD_ENDPOINT_ID"
chmod 0600 "$work/RUNPOD_API_KEY" "$work/RUNPOD_ENDPOINT_ID"

ssh -i ~/.ssh/deployment -o IdentitiesOnly=yes \
  ubuntu@"$DEPLOY_SSH_HOST" \
  docker service scale tars_dispatcher=0
dispatcher_paused=1

python3 scripts/tars/tars_runpod_release.py migrate \
  --api-key-file "$work/RUNPOD_API_KEY" \
  --endpoint-id-file "$work/RUNPOD_ENDPOINT_ID" \
  --ids-output "$work/stable-ids.json" \
  --confirm-adopt-existing-resources

for key in \
  RUNPOD_ENDPOINT_ID \
  RUNPOD_TEMPLATE_ID \
  RUNPOD_REGISTRY_AUTH_ID
do
  jq -er --arg key "$key" '.[$key]' "$work/stable-ids.json" \
    > "$work/$key"
  grep -Eq '^[A-Za-z0-9_-]{1,191}$' "$work/$key"
done

infisical secrets set \
  "RUNPOD_ENDPOINT_ID=@$work/RUNPOD_ENDPOINT_ID" \
  "RUNPOD_TEMPLATE_ID=@$work/RUNPOD_TEMPLATE_ID" \
  "RUNPOD_REGISTRY_AUTH_ID=@$work/RUNPOD_REGISTRY_AUTH_ID" \
  --domain https://secrets.lascade.com \
  --projectId a05e73f7-f45a-43af-8ddd-3d5b4f8bd8e5 \
  --env prod \
  --path /deployment \
  --silent >/dev/null

ssh -i ~/.ssh/deployment -o IdentitiesOnly=yes \
  ubuntu@"$DEPLOY_SSH_HOST" \
  docker service scale tars_dispatcher=1
dispatcher_paused=0
trap - EXIT HUP INT TERM
rm -rf -- "$work"
)
```

The migration command refuses
unknown ownership, shared dependencies, non-immutable images, the old
single-pool GPU contract, queued jobs, or active workers. It is safe to rerun
if the template rename completed but the endpoint rename response was lost.
The exit trap restores the dispatcher and removes the owner-only temporary
files if any command fails; do not bypass a reported restore failure. After the
block succeeds, run the normal `main` deployment.

## Greenfield bootstrap only

`bootstrap` is the sole creation path and is only for an account with no
existing TARS Runpod resources:

```sh
python3 scripts/tars/tars_runpod_release.py bootstrap \
  --release-sha "$RELEASE_SHA" \
  --gpu-image "$IMMUTABLE_GPU_IMAGE" \
  --api-key-file "$work/RUNPOD_API_KEY" \
  --registry-username-file "$work/DOCR_READ_USERNAME" \
  --registry-password-file "$work/DOCR_READ_PASSWORD" \
  --ids-output "$work/stable-ids.json" \
  --confirm-create-stable-resources
```

Store the emitted IDs in Infisical `/deployment` before the first normal
deployment. Never put `bootstrap` in CI.

## Ordinary deployment and rollback

The delivery workflow:

1. Confirms the release is still current `main`.
2. Captures the live dispatcher release label, exact
   `TARS_GPU_IMAGE_DIGEST`, and stable endpoint ID. Before scaling the
   dispatcher down, it atomically installs an owner-only application boundary
   at `/srv/tars/deployment/runpod-rollout-boundary.json`; only then does it
   drain. A missing dispatcher is accepted only as an explicit greenfield state
   with no other application services, deployment lock, or current release
   record.
3. Runs `prepare` against the fixed endpoint/template/auth IDs. For an existing
   deployment it proves that the provider image is byte-for-byte the same
   immutable tag-plus-digest recorded by the live dispatcher. For greenfield it
   proves that the explicit bootstrap already points at the candidate image.
4. Atomically replaces the application boundary with the prepared rollout
   receipt before any provider mutation. The path is never absent during this
   transition. The receipt binds the prior app SHA and GPU image, candidate SHA
   and GPU image, provider image and endpoint version, and all three stable
   provider IDs.
5. Runs `stage`, which re-reads that exact boundary, waits for zero queued or
   in-progress jobs and only inactive workers, updates the fixed template at
   most once, and proves the endpoint converged to the same candidate SHA and
   digest. The confirmed provider version is copied back to the durable marker.
6. Deploys TARS and runs the paid authenticated smoke render.
7. After `tars-deploy` records the smoke-accepted current release, the workflow
   re-reads Runpod and proves the exact target image/version is still idle,
   verifies the live dispatcher SHA, GPU digest, and endpoint ID, and removes
   the marker.

If failure or cancellation happens before the marker transition, the handler
uses the application boundary to restore the accepted app. Before re-enabling
its dispatcher it also reads Runpod and proves that the configured stable
endpoint, template, registry authorization, image, version, and every active
worker still match that application boundary. After the transition, TARS first
attempts its application rollback. The handler reads the control-node rollout
receipt rather than trusting runner-local state, proves that any deployment
lock is absent or owned by that exact candidate before provider mutation,
drains the dispatcher, restores and verifies the exact prior provider image,
and repairs the prior application from its SHA-addressed bundle when necessary.
It rechecks the prior app SHA, GPU digest, endpoint ID, current release record,
provider image, and a healthy dispatcher before deleting the marker. If any
proof fails, the dispatcher stays paused and the marker remains for the next
deployment or an operator.

A replacement runner always reconciles an existing marker before preparing a
fresh release. It verifies deployment-lock ownership before provider
reconciliation and re-reads the provider after any application repair, just
before restoring the dispatcher. This keeps runner loss from turning a
runner-local receipt into an untracked or mismatched provider mutation.

On a first greenfield deployment there is intentionally no previous
application to restore. Ordinary CI uses the already bootstrapped fixed
resources, performs a no-op provider stage when their image is already the
candidate, and deploys the application normally. Recovery either proves the
smoke-accepted candidate from the current release record or proves that no app,
current record, or deployment lock exists; it never invents a prior app.

TARS housekeeping validates this same owner-only marker before pruning. An
application boundary protects its release SHA. A full rollout receipt protects
both its prior and target SHAs, including stored and incoming recovery bundles.
Malformed or unsafe live markers stop housekeeping before deletion.

Each transient HTTP operation is bounded to three calls. A template update or
endpoint rename is issued once; an ambiguous lost response is reconciled by
read-only polling rather than by blindly repeating the mutation.

## Retire old per-release resources explicitly

Legacy resource deletion is never part of CI. After a stable deployment has
passed smoke acceptance, create an exact owner-only retirement plan:

```sh
python3 scripts/tars/tars_runpod_release.py retire-legacy-plan \
  --api-key-file "$work/RUNPOD_API_KEY" \
  --endpoint-id-file "$work/RUNPOD_ENDPOINT_ID" \
  --template-id-file "$work/RUNPOD_TEMPLATE_ID" \
  --auth-id-file "$work/RUNPOD_REGISTRY_AUTH_ID" \
  --plan-output "$work/legacy-retirement.json"
```

Review the plan and pass back the printed SHA-256 exactly:

```sh
python3 scripts/tars/tars_runpod_release.py retire-legacy \
  --api-key-file "$work/RUNPOD_API_KEY" \
  --plan-file "$work/legacy-retirement.json" \
  --confirmation-sha256 "$REVIEWED_PLAN_SHA256"
```

Retirement protects the stable IDs, refuses changed or shared inventory, and
preflights every legacy endpoint for queued jobs and active workers before its
first mutation. The exact same plan is safe to rerun after partial cleanup.

Runpod API contracts used here:

- [Update a template](https://docs.runpod.io/api-reference/templates/POST/templates/templateId/update)
- [Update an endpoint](https://docs.runpod.io/api-reference/endpoints/PATCH/endpoints/endpointId)
- [Inspect endpoint workers](https://docs.runpod.io/sdks/graphql/manage-endpoints)
