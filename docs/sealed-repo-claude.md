## Sealed output

This repo is public and its CI output is sealed. Rules, enforced by `.github/workflows/seal-policy.yml`:

- Every job starts with `Lascade-Co/actions/actions/seal@seal-v1` and ends with `Lascade-Co/actions/actions/seal-upload@seal-v1` (`if: always()`). Neither takes `with:`.
- Never use plain `shell: bash`; the default shell is `seal-bash {0}` or `seal-bash-e {0}`.
- Never expand private values into `run:` text, `env:` or a third-party `with:`; the public step header shows them.
- Pack artifacts with `seal-pack` and read them with `seal-unpack` (`REPO_AGE_KEY` from secrets); keep retention at 7 days or less.
- Mask private values with `::add-mask::` in an earlier sealed step; never mask public metadata.
- No secrets in `pull_request` workflows; no `pull_request_target`.
- Jobs with a `permissions:` block need `contents: read`.
- Full rules: https://github.com/Lascade-Co/actions/blob/main/docs/sealed-output.md
- Open runs at https://ci.lascade.com/?repo=<repo> (staff sign-in through Cloudflare Access; no token or key to paste).
