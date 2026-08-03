# Lascade Actions

The shared vocabulary for this repo's pipelines. Two have enough domain language to need it:
**Daily Catchup** and **Blog SEO Audit**.

## Language — Daily Catchup

The org-wide daily engineering report pipeline (`.github/workflows/daily-catchup.yml`):
discover active repos → summarise each with Codex → merge into one daily file → commit it
to the `catchup` repo and email a styled report.

**Catchup**:
The daily pipeline (and the `Lascade-Co/catchup` data repo) that captures the org's last 24h
of engineering activity.
_Avoid_: digest, standup.

**Daily file**:
The merged `daily/YYYY-MM-DD.json` — one entry per active repo with its developers, bullets,
and enrichment. The single source the commit and email steps both consume.
_Avoid_: report (that's the email), summary (that's per-repo).

**Per-repo summary**:
One repo's `summary-*.json` artifact: developers with Codex bullets plus enrichment.
Produced by the `summarize` matrix job.

**Enrichment**:
The non-commit signals attached to each repo summary — merged **PRs**, active **branches**,
the in-window release **version** tag, and the repo's **tags**. All best-effort.

**Status**:
A commit's delivery state, decided deterministically from branch/PR state in the per-repo
step (never by Codex): **Published** (reachable from the default branch), **Testing** (on a
branch with an open PR), **Work in Progress** (on a branch with no PR). Bullets are grouped
by status, and these are the email's section headings.
_Avoid_: Shipped, In Progress (earlier freeform names — superseded by these three)._

**Exclude list**:
`data/catchup_exclude.txt` — owner/repo names omitted from the daily email (one per line,
`#` comments allowed). A repo is **included by default**; add it here to opt out. Excluded
repos are still summarised and committed to the daily file. Seeded from repos lacking the
`catchup-mail` topic, then maintained by hand — the runtime no longer reads topics.

**Active repo**:
A repo with non-bot commits in the look-back window — the only repos that get summarised and
appear in the daily file. (There is no "inactive" list in the email.)

**Report JSON**:
The email's intermediate `report.json` — Codex prose (executive summary, display names,
patterns) merged with authoritative numbers and the deterministic **status** sections. The
renderer turns it into the HTML email.

**Authoritative**:
A value not trusted from Codex — computed from git/GitHub (commit counts, contributor list,
PR count, version, branches, stats) or derived deterministically (the **status** split).
Codex supplies prose only (bullets, executive summary, display names, patterns).

## Relationships — Daily Catchup

- The **discover** job lists **active repos**; **summarize** turns each into a **per-repo
  summary** with **enrichment**.
- **collect** merges all per-repo summaries into the one **daily file**.
- **commit** publishes the daily file to the **catchup** repo; **email** drops repos in the
  **exclude list**, builds the **report JSON**, and sends it. commit and email run in parallel.

## Language — Blog SEO Audit

The daily cron audit (`.github/workflows/seo-blog-audit.yml`) that checks the newest blogs on
each configured site against a fixed rule set and sends `report.html` to Telegram when
something is wrong.

**Blog**:
One article published at `www.<domain>/hub/<slug>`.
_Avoid_: post, article, hub page.

**Blog listing**:
The paginated index at `www.<domain>/hub` that an audit run reads to decide which blogs to
check.
_Avoid_: hub, blog index, archive.

**CMS**:
The WordPress installation where blogs are authored, reachable at the **origin** as a REST API
under `/wp-json/`.
_Avoid_: WordPress (in code identifiers), backend, hub.

**Origin**:
The `hub.<domain>` host the **CMS** serves from. Every non-asset path on it redirects to the
canonical host.
_Avoid_: hub host, CMS host, WP host.

**Asset URL**:
An **origin** URL under `/wp-content/uploads/` — the only class of origin URL allowed to reach
a crawler.
_Avoid_: static asset, upload, media URL.

**Crawlable position**:
A place in served HTML where a URL is a navigational signal to a crawler — `a[href]`,
canonical, `og:url`, hreflang, JSON-LD `url`/`@id`/`item`, sitemap `loc`. A URL merely present
elsewhere in the markup is not in a crawlable position.
_Avoid_: visible link, public link.

**Rule**:
One named check with a fixed ID (`A1`…`G5`), a slug, and a **severity**.
_Avoid_: test, validation, assertion.

**Finding**:
One instance of a **rule** firing against one **blog**.
_Avoid_: violation, issue, error (that's a severity).

**Severity**:
A rule's weight — **error** (crawl or index correctness is actively broken), **warn**
(threshold or hygiene), **info** (recorded for visibility, expected to be non-empty). Errors
and warns gate delivery; info never does.

**Suppressed rule**:
A rule named in a **site config**'s `suppress` list — still evaluated and still shown in the
report, but unable to trigger delivery. For known, accepted conditions.
_Avoid_: disabled, ignored, muted.

**Site config**:
One entry in `data/seo_sites.json` — canonical host, **origin**, asset path prefixes, allowed
sibling subdomains, listing path, sitemap URL, blog count, thresholds, suppress list.

**Audit run**:
One execution of the audit for one **site config**. Sites run as parallel matrix jobs and
never share a report.

## Relationships — Blog SEO Audit

- A **site config** names exactly one **origin** and one **blog listing**.
- The **blog listing** yields the **blogs** an **audit run** checks (the first N in DOM order).
- Each **rule** yields zero or more **findings** per **blog**; **severity** decides whether an
  audit run delivers its report.
- **Origin** URLs are legitimate only as **asset URLs**. Any other origin URL is a **finding** —
  `error` when in a **crawlable position**, `warn` when merely present in the markup.

## Example dialogue

> **Dev:** "How do I keep a noisy repo out of the email?"
> **Maintainer:** "Add it to data/catchup_exclude.txt. It's still summarised and committed
> to the daily file — it just won't show up in the *email*."

> **Dev:** "The A2 finding says the **origin** is leaking. Is the **blog listing** broken?"
> **Maintainer:** "No — the **blogs** render fine. The Next.js payload embeds **CMS** API URLs
> in the served HTML, so the origin is exposed in the markup but not in a **crawlable
> position**. That's why it's `warn` and not `error`. If it ever shows up in an `a[href]`,
> that's A1 and it's an `error`."

## Flagged ambiguities

- "summary" meant both the per-repo artifact and the emailed report — resolved: **per-repo
  summary** is the artifact, **report JSON** / email is the org-wide output.
- "hub" meant three things — the CMS host, the blog index page, and the articles themselves.
  Resolved: **origin** is the host, **blog listing** is the index, **blog** is one article, and
  **CMS** is the authoring system. The word "hub" survives only as the literal URL path
  `/hub`, never as vocabulary.
