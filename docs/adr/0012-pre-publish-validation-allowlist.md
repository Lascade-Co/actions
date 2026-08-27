# Pre-publish validation runs an allowlist of the audit's rules, not the audit

Before a draft is written to the CMS, its HTML is wrapped in a synthetic host page, parsed by
`seo_parse.parse_blog()` and run through **18 named rules from the Blog SEO Audit** — not the audit,
and not a second checker written for the purpose. The set is an explicit allowlist in
`release_blog_check.py`:

`A1 A2 A4 A5 · B7 · C1 C4 · D1 D2 D3 D4 D5 D6 · E1 E4 · G1 G2 G4`

**Why reuse the rules.** The repo already encodes what a good blog looks like, with thresholds
(`title_max`, `description_min`, `word_count_min`, `internal_links_min`) and a generic-anchor-text
list that took real findings to tune. A parallel checker would drift from it, and the drift would
show up as blogs that pass pre-publish validation and then fail the daily audit — the two systems
disagreeing about the same page. Reuse costs nothing: every check has the signature
`check_x(page, site, urls, ctx)`, and these 18 read neither `urls` nor `ctx`, so they run offline
against an empty URL map and a bare `SiteContext`.

**Why an allowlist rather than the whole set.** Two thirds of the catalogue cannot answer at this
point in time, for two different reasons, and both matter:

- **Needs the network or the live site** — `A3 B1 B2 B3 B4 B5 B6 G3 G5 D7 C3 H1 H2 H3 I1 I2 I3 I4`.
  A draft has no URL, is not in the sitemap, is not on the listing, and has no response headers. These
  are the daily audit's job, after publish.
- **Not the draft's output** — `E2 E3 E5 E6 F1 F2 F3 F4 D8 C2`. The theme and the Next.js front end
  own `<title>`, canonical, OG tags, Twitter tags, `lang`, viewport and Article schema. The draft
  owns a title, a slug, an excerpt and a body. Asserting the rest here would validate this pipeline's
  own synthetic scaffolding, or demand JSON-LD the content contract deliberately forbids so the
  published page does not carry two copies of its Article schema.

**`D9` is excluded even though it could run.** It is pure-HTML and would pass the eligibility test
above, but placeholders are correct in a draft — see ADR-0011. A rule being *runnable* offline is not
the criterion; being *answerable about a draft* is.

**The trap.** Adding a rule to the allowlist looks obviously correct and is how this design breaks.
A rule that reads `urls` sees an empty map and silently reports nothing — no error, no test failure,
just a check that quietly stopped checking. A rule that reads the synthetic head reports on
scaffolding this pipeline wrote. Anything added here needs the same two questions asked of it: does
it read only the parsed page, and can it be answered about something nobody has published?

**Findings report, they do not gate.** `error` and `warn` trigger exactly one retry, with the
specific findings and the previous attempt fed back. The better attempt by (errors, warns) is then
published *regardless of whether it passes* — an editor holding a named list of three warnings is
better served than an editor holding nothing, and `validation.txt` carries both attempts either way.
`info` is inert here exactly as it is in the audit (ADR-0003): `A5` fires on every legitimate link to
a sibling subdomain, so scoring it would burn the retry on correct output. A **suppressed rule** is
evaluated and reported but cannot trigger the retry or affect scoring, which generalises suppression
from "cannot trigger delivery" to "cannot trigger its pipeline's consequence" — a `suppress` entry
means "accepted on this site" wherever it is read.

**One retry, not a loop.** Each attempt is a Codex call on a release-path job. A loop that converges
slowly, or does not converge, turns a content nicety into a build-minute sink, and the second attempt
is where nearly all of the gain is: the first pass misses a threshold, the fed-back finding names it
exactly.

**Considered and rejected.** *Run the full audit against a preview URL* — correct in principle and
impossible in practice: the draft has no URL until it is published, which is the thing being gated.
*A hard gate that refuses to POST on any error* — loses a fixable draft entirely and moves the work
back to a human who now has nothing to start from. *A purpose-built lightweight checker* — the drift
problem above.
