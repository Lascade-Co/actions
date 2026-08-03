# The Blog SEO Audit reads the CMS API as a second source of truth

Rule group `I` of the Blog SEO Audit calls `{origin_host}/wp-json/wp/v2/posts` — the same CMS REST
endpoint that rule `A2` flags as origin exposure when it appears in served markup. This looks
self-contradictory and is deliberate.

**Why.** Every other rule in the catalogue reads only what `www` renders, which makes an entire
failure class invisible. If a blog is `status: publish` in the CMS but the Next.js build never
picked it up, it 404s or is missing from the blog listing — and no `www`-side rule can fire,
because the audit discovers blogs *from* that listing. The bug hides inside the discovery step
itself: the audit cannot miss what it never knew to look for. The same blindness covers a blog
edited in the CMS while `www` serves a stale render, and a blog unpublished in the CMS that stays
live as a zombie page. Detecting any of these requires a second source of truth, and the CMS is the
only one that exists.

**The apparent contradiction resolves cleanly.** `A2` is about what *crawlers* can see in served
HTML — the origin should not be discoverable through the public front end. The audit is not a
crawler and does not publish what it fetches. Reading the endpoint directly is exactly what a
first-party monitor should do; leaking it into a page's markup is not.

**Consequences.** Discovery becomes the union of the blog listing's first `blog_count` and the CMS's
first `blog_count` by date — a blog present in one and absent from the other *is* the finding — so a
run fetches roughly 12 pages rather than 10. The audit is now coupled to WordPress REST field names
(`slug`, `date`, `modified`, `status`), which a CMS migration would break.

**That coupling is contained deliberately.** Group `I` is gated per site by `"cms_api": true`, and
any failure of the call — non-200, timeout, schema change, an authentication wall added later —
skips the whole group and records a single `info` finding (`I4`) naming the underlying error. A CMS
that stops answering must never look like a broken website, and must never page anyone. The audit
degrades to `www`-side-only checks and says so in the report.
