# Vendored libraries

The viewer loads no code from a CDN. These files come from npm registry tarballs.
`SHA256SUMS` holds their hashes; check them with `shasum -a 256 -c SHA256SUMS`.

| File | Source | How |
| --- | --- | --- |
| `fflate.js` | `fflate@0.8.3`, `esm/browser.js` | copied unchanged |
| `ansi_up.js` | `ansi_up@6.0.6`, `ansi_up.js` | copied unchanged |
| `age-encryption.js` | `age-encryption@0.3.1` and its dependencies | bundled with esbuild 0.25.12 (below) |

Registry integrity of the three top-level tarballs:

- `age-encryption@0.3.1` `sha512-bYgd7lxM7tEANmb9bXf7xTFB3Qpq+JGKinVSdFh5MV+t2fJJZSTdKhWkvXTFJQGysZd+K9Dt3F+n+4DKazXlnQ==`
- `fflate@0.8.3` `sha512-tbZNuJrLwGUp3zshBtdy4W+ORxZuIh8a5ilyIEQDC5rY1f3U20JMry0Ll3WBzU58EZKsEuJFXhb5gwv8CsPvgA==`
- `ansi_up@6.0.6` `sha512-yIa1x3Ecf8jWP4UWEunNjqNX6gzE4vg2gGz+xqRGY+TBSucnYp6RRdPV4brmtg6bQ1ljD48mZ5iGSEj7QEpRKA==`

`age-encryption` imports `@noble/*` and `@scure/*` modules, so it is bundled into
one self-contained ES module. npm resolved these dependency versions:

- `@noble/ciphers@2.4.0`
- `@noble/curves@2.4.0`, plus `@noble/curves@2.0.1` nested under `@noble/post-quantum`
- `@noble/hashes@2.4.0`, plus `@noble/hashes@2.0.1` nested under `@noble/post-quantum`
- `@noble/post-quantum@0.5.4`
- `@scure/base@2.4.0`

Build steps, run in an empty temp directory:

```sh
npm init -y
npm install --ignore-scripts age-encryption@0.3.1 fflate@0.8.3 ansi_up@6.0.6
printf 'export * from "age-encryption";\n' > entry.mjs
npx --yes esbuild@0.25.12 entry.mjs --bundle --format=esm --platform=browser \
  --target=es2022 --legal-comments=inline --outfile=age-encryption.js
```

To update a library, rebuild with pinned versions, review the diff, and regenerate
`SHA256SUMS`. The page uses only fflate's synchronous APIs. Its async APIs start
`blob:` workers, which the page's CSP blocks.
