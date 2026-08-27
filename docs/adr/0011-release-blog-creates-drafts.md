# The Release Blog pipeline creates drafts, never published blogs

`.github/workflows/release-blog.yml` turns a release diff into an SEO-validated post in the site's
CMS and stops there: `status: draft`, every time, with no path to publishing. An **editor** reviews
it and publishes. The pipeline holds credentials that could publish and deliberately does not use
them.

**Why.** The output is AI-written marketing copy about a product, on a domain whose search
performance the business depends on. Every other Lascade pipeline that reaches the outside world
sends to an internal surface — Telegram, email, a data repo — where a bad output costs someone thirty
seconds of confusion. A blog is public, indexed, and attributed to the brand. The failure mode is not
"a wrong number in a chat message", it is a page claiming a feature that does not exist, ranking for
it, and being read by customers. A human between the model and the public is the entire point, and
it is cheap: reviewing a draft takes minutes and the draft has already done the writing.

**Placeholders are legitimate in a draft and forbidden in a blog.** Media is emitted as sized
`placehold.co` **placeholders** carrying the real image's description as `alt` plus a generation
brief, and
the draft opens with an editor checklist enumerating them. That is the correct state for something
nobody has published. Once published it is a fake image on a live page, so rule `D9`
(`placeholder-media-published`, `error`) was added to the Blog SEO Audit to catch `data-placeholder`
and `data-strip-before-publish` in served HTML. This is the one condition the repo checks on both
sides of publication, with opposite verdicts — the pre-publish rule set permits it, `D9` does not.
Without `D9` the pipeline's most likely real-world failure had no detector at all: a `placehold.co`
image returns 200 with `image/png`, so `B3` passes it and no rule constrains an image's host.

**A re-run overwrites its own draft; a published match stops the run.** Every draft closes with
`<!-- release-blog: <owner>/<repo>@<tag> -->`. Before generating, the pipeline searches the CMS for
that marker. Found on a draft — overwrite it in place, because a re-run means the release was
re-cut and a second near-identical draft leaves the editor guessing which is current. Found on a
published blog — stop, produce nothing: that release already has a live page and regenerating over
it would push unreviewed copy onto an edited public blog. Keying on the marker rather than the slug
matters because the slug is the writer's SEO choice and varies between runs of the same release.

**The authenticating account is the public byline.** WordPress attributes a post to whichever user
the application password belongs to, and the front end feeds that into the visible byline and into
`author` in the Article schema — which `E2` requires. So `CMS_USER` is not a service identity: it is
a properly-named editorial account with a real display name, bio and avatar ("Travel Animator Team"),
because whatever it is called ends up printed on every blog this pipeline wrote. Setting `author`
explicitly in the POST was the alternative; it needs `edit_others_posts` on the bot and a WordPress
user ID hardcoded in repo config, to solve a problem that goes away by naming the account correctly
once.

**Nothing here can fail a release.** The CLI always exits 0 and the caller's job is
`continue-on-error: true`. This extends ADR-0003's reasoning from a daily cron onto the release path,
where the stakes are higher in both directions: a red X on a release that actually shipped
successfully is worse than a missing blog, and a release blocked because WordPress was down would be
an absurd coupling. Artifacts upload on every run, including skips, so the pipeline's own health is
inspectable without asking the CMS.

**Considered and rejected.** *Auto-publish* — the whole objection above. *A PR into a content repo*
— versioned and reviewable, and genuinely attractive, but it puts the review in the wrong tool for
the person doing it: editors work in WordPress, not in pull requests, and a diff of HTML is a poor
review surface for prose. *An artifact attached to the GitHub Release* — zero blast radius and zero
adoption; nobody opens a workflow artifact to find their next blog post. *Abstaining on
internal-only releases* — a silent skip is indistinguishable from a broken pipeline, and the release
notes already state whether anything user-visible shipped, so the editor can make that call with
better information than a diff heuristic has.

**Consequence.** Drafts accumulate if nobody reviews them, one per release, and nothing in this repo
notices. The pipeline never looks: it searches only for the marker of the release in hand, so it has
no idea whether last month's drafts were published, binned, or ignored. If drafts quietly stop being
published, the symptom is silence — no red run, no Telegram message, just a `/hub` that stops
gaining pages.
