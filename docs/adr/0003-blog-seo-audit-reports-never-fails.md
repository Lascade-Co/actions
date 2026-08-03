# The Blog SEO Audit reports and never fails

`.github/workflows/seo-blog-audit.yml` runs a daily SEO audit over the newest blogs on each
configured site. **It always exits 0.** No finding, at any severity, ever turns the run red.
Delivery of `report.html` to Telegram is the only signal, and it fires only when the run produces
at least one `error` or `warn` finding from a rule that is not suppressed.

**Why.** The audit checks a live website, not a build artifact. Its inputs are third-party HTTP
responses, a WordPress instance, and Vercel's ISR cache — all of which move without anyone
touching this repo. A red X on `main` that nobody caused and nobody can fix by reverting trains
people to ignore red X's on `main`, which is a far more expensive outcome than a missed SEO
warning. Findings here are editorial work items, not broken builds.

**Severity gates delivery, and `info` never gates it.** Three rules are expected to be non-empty
on a perfectly healthy site: `A5` fires on every legitimate link to a sibling subdomain (`support.`,
`model.`, `viral.`), `B6` on any dead external link the world broke for us, and `G5` on
time-to-first-byte measured while the audit itself hammers ten pages concurrently. Gating on "any
finding" would have paged daily on an all-clear run — the fastest possible way to make the channel
worthless. So `info` is report-only context that can never trigger a send.

**Suppression is config, not code.** `data/seo_sites.json` carries a per-site `suppress` list of
rule IDs. A suppressed rule still evaluates and still appears in the report under its own heading
with counts; it just cannot trigger delivery. The mechanism was introduced for `A2` on travelanimator,
whose origin exposure looked structural — the Next.js flight payload serialises CMS API responses
into the markup. **That entry was removed on 2026-08-04**: the evidence came from the `/hub`
listing page, and the audit runs rules only against blog pages, where A2 fires 0/0. The mechanism
stays because the reasoning still holds for any rule whose condition is genuinely permanent; it
currently has no subject. That reasoning: alerting daily on a condition nobody intends to fix this
quarter ends with a muted chat and an audit that is decorative. Suppressing a rule, and later
un-suppressing it, is a one-string edit to a JSON file — a deliberate act that shows up in a diff
rather than a silent forget.

**Considered and rejected.** *Fail on error-severity findings* — attractive until the first
`hub.<domain>` asset 404 caused by a WordPress media edit paints `main` red for a content change.
*Fail on any finding* — same problem, daily. *Diff against a committed baseline and alert only on
new findings* — precise, but it needs write-back to the repo on every run and it silently stops
re-reporting long-standing breakage, so a real defect goes quiet after day one.

**Consequence.** Nothing in GitHub's UI signals a problem — the Actions tab is green by design. The
Telegram chat is the sole alerting surface, so if that bot token expires or the chat is left, the
audit goes silent while still reporting success. `report.html` is therefore uploaded as an artifact
on **every** run, including clean ones, so the audit's own health can be checked without depending
on the delivery path. A run that throws unexpectedly still renders a report carrying the traceback
as an `H3` finding and still delivers it; a silently-green cron is the specific failure mode this
design guards against.
