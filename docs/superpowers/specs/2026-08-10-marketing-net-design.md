# Marketing Net — daily figure to Telegram

A daily cron that posts one number to a Telegram group: month-to-date app revenue minus
month-to-date marketing spend. Nothing is persisted anywhere.

Vocabulary for this pipeline lives in [`CONTEXT.md`](../../../CONTEXT.md) under **Marketing Net**.
Three decisions carry their own ADRs: [0005](../../adr/0005-marketing-net-recomputes-revenue.md)
(why revenue is recomputed rather than read from the PNL app),
[0006](../../adr/0006-single-rate-date.md) (one FX rate date per run), and
[0007](../../adr/0007-play-net-factor.md) (Play is calibrated sales, not gross).

## The figure

```
Marketing net = (App Store + Play Store) − (Influencer + Google Ads + Meta Ads)
```

Called **marketing net**, never "profit" — the PNL app already defines `gross_profit` and
`contribution_profit` by different formulas, and a third thing called profit invites a false
reconciliation.

## Sources

| Source | Origin | Window |
| --- | --- | --- |
| Influencer | `GET pnl.lascade.com/api/head-spend/?head=INFLUENCER%20MARKETING` | calendar month, server-side |
| App Store | App Store Connect `/v1/salesReports`, one request per day | 1st → **yesterday** |
| Play Store | GCS `sales/` × net factor from `earnings/` | 1st → today |
| Google Ads | Google Ads API **v25**, MCC children minus skip list | 1st → today |
| Meta Ads | Graph API **v26.0** `/act_<id>/insights` | 1st → today |

Windows genuinely differ and the message states them rather than clipping Play and the ad platforms
back to the App Store's boundary — that would discard real data.

### Influencer spend

`X-Api-Key: $PNL_API_KEY`. `spend_usd` is a **string at four decimal places, always** — parse as
`Decimal`, never `float`. The figure already excludes recurring spends, counts `enabled` rows only,
and is signed (a refund-positive row reduces the line). Do not re-derive or adjust it.

`404` means the configured head key is wrong, **not** that the month is quiet. Surface it; never
treat it as `0`.

The key contains a **space**: PNL derives keys from card statement descriptors, so the shape is
`AWS BILL`, `INFLUENCER MARKETING`. The underscored spelling `INFLUENCER_MARKETING` 404s — which is
the rule above earning its keep, since the alternative was reporting `$0` indefinitely.

### App Store

ES256 JWT (`iss`/`iat`/`exp`/`aud: appstoreconnect-v1`, `kid` in header) signed with the `.p8`.
Response is gzipped TSV; sum `Developer Proceeds × Units` per `Currency of Proceeds` — proceeds are
**per unit**, and units are signed so refunds carry negatives.

- A `404` day is a genuinely sale-less day *or* one Apple has not published yet. Count it as zero
  for that day, do not abort, and **do not cache it**.
- A published day is immutable — cache aggressively via `actions/cache` keyed by month.
- Today is never available; reports publish next-day around 05:00 PT.
- An unrecognised TSV layout parses to zero rows with a `200`. Warn on an unexpected zero across a
  multi-day window rather than reporting a silent `$0`.

### Play Store

Per ADR-0007. Bucket `pubsite_prod_rev_<account_id>`, scope `devstorage.read_only`. Month extracted
from earnings filenames with a regex on `_(\d{6})`, never `split("_")[-1]`.

### Google Ads

Include-by-default with exclusions. Two queries: list the MCC's children, then per surviving child
fetch month-to-date cost.

```sql
SELECT customer_client.id, customer_client.currency_code
FROM customer_client
WHERE customer_client.manager = FALSE AND customer_client.status = 'ENABLED'

SELECT metrics.cost_micros, customer.currency_code
FROM customer
WHERE segments.date BETWEEN '<1st>' AND '<today>'
```

`customer_client.manager = FALSE` already excludes the MCC itself, but `skip_customer_ids` is applied
explicitly regardless — the list is the stated intent and must not depend on a filter coincidence.
`cost_micros ÷ 1_000_000`, in the customer's own currency.

### Meta Ads

Explicit include list — only the accounts named in `account_ids`. Request `spend` **and
`account_currency`**; spend arrives as a string. Should the credential file ever grow multiple
groups, each carries its own token and an expired token degrades only its own group.

## Failure model

Every source yields either an amount or **`Unavailable(reason)`**. There is no third state, and
`Unavailable` is **never** coerced to `0` — a plausible zero is indistinguishable from a genuinely
quiet month, which is the failure this whole design guards against.

| Condition | Behaviour | Run |
| --- | --- | --- |
| Any single source fails | line shows `unavailable`, warning names it, net excludes it | green |
| Empty App Store window (the 1st) | `unavailable`, warned as `no published days in the window yet` | green |
| A currency in a source has no USD rate | that **whole source** is `unavailable`, warning names the codes | green |
| **No** revenue source readable | net itself renders `unavailable`, spend lines still shown | green |
| Telegram send fails after retries | — | **red** |

Currency conversion is **all or nothing per source**. Converting the priceable
subset and reporting the remainder would produce a figure smaller than the truth and
entirely plausible — the one failure a reader cannot detect. Refusing the whole source is loud.

This is ADR-0003's rule, not a departure from it: a run is green when it produces meaningful output
and red when infrastructure fails. Source failures still produce a message; a dead delivery path
produces nothing, and there is no message left to carry the warning.

## Message

Right-aligned columns require `<pre>` — Telegram renders everything else proportionally.

```
Marketing net — Aug 1–10

Revenue
  App Store      $12,430   (to Aug 9)
  Play Store     $ 8,110
  Total          $20,540

Spend
  Influencer     $   350
  Google Ads     $ 4,220
  Meta Ads       $ 3,015
  Total          $ 7,585

Net              $12,955
```

Four rendering rules, each learned the hard way:

- **Escape `&`, `<`, `>`** in any interpolated exception text. Otherwise `400 can't parse entities`
  and nothing is delivered — on precisely the days a source broke and the warning mattered most.
  Quote escaping is unnecessary; the text sits in element content, never an attribute.
- **Truncate at 4096 characters on a line boundary**, never mid-entity, or a half-written `&am`
  reproduces the failure being defended against.
- **Round once.** Quantize revenue and spend, then derive the shown net from the rounded pair, or
  the column visibly fails to subtract.
- **Check the sign before rounding**, or a small negative renders as `$-0`.

**Never log the bot token.** It sits in the URL path and `str(ConnectionError)` embeds the full
request URL; redact before anything derived from an exception reaches a log.

## Modules — `scripts/pnl/`

Following the `scripts/seo/` precedent: focused modules, fetched by raw URL, offline tests.

| Module | Responsibility |
| --- | --- |
| `pnl_money.py` | Decimal parsing, round-once, negative-zero guard |
| `pnl_fx.py` | native → USD at the single rate date |
| `pnl_spend.py` | PNL `head-spend` client |
| `pnl_appstore.py` | JWT, daily `salesReports`, gzipped TSV |
| `pnl_playstore.py` | GCS `sales/` + `earnings/` net factor |
| `pnl_googleads.py` | Google Ads v25 |
| `pnl_metaads.py` | Graph v26.0 |
| `pnl_telegram.py` | render, escape, truncate, redact, send |
| `marketing_net.py` | orchestrator |

## Workflow

`.github/workflows/daily-marketing-net.yml` — `schedule: '0 14 * * *'` plus `workflow_dispatch`.
14:00 UTC, not 13:00: under PST, Apple's 05:00 PT publication *is* 13:00 UTC exactly, so 13:00 sits
on the publication boundary. Earlier still means every message carries "1 day unavailable" forever
and the App Store contributes `$0` on the 2nd of each month.

No `repository_dispatch`, so no trigger file is needed.

Secrets arrive via `Infisical/secrets-action@v1.0.16` with `project-slug: pnl`, `env-slug: prod`,
`export-type: env`. The Telegram chat id is hardcoded in the workflow (matching
`seo-blog-audit.yml`); the bot token comes from GitHub secrets.

Config is validated at startup and the run fails with a message **naming the missing key** rather
than failing obscurely at first use.

### Secrets

All verified present in Infisical `pnl`/`prod` on 2026-08-10.

| Key | Status |
| --- | --- |
| `PNL_API_KEY` | present |
| `APPSTORE_ISSUER_ID`, `APPSTORE_KEY_ID`, `APPSTORE_P8_B64`, `APPSTORE_VENDOR_NUMBER` | present |
| `PLAYSTORE_SA_JSON_B64`, `PLAYSTORE_BUCKET` | present |
| `ADS_CREDENTIALS_JSON_B64` | **created 2026-08-10**, verified to decode |
| `TELEGRAM_BOT_TOKEN` | present |

**The chat id must not be read from `TELEGRAM_CHAT_ID`.** That key already exists in `pnl`/`prod`
and belongs to the PNL app's own alerting. `export-type: env` puts it in the job environment, so a
script reading `TELEGRAM_CHAT_ID` would silently deliver to PNL's chat instead. The workflow sets
**`MARKETING_NET_CHAT_ID: '-5466007383'`** and the script reads only that name — a distinct name is
the defence, because the inherited value is otherwise perfectly valid and would fail silently by
succeeding.

`ADS_CREDENTIALS_JSON_B64` holds one blob so that adding an account is an edit to one value:
`google` carries `client_id`, `client_secret`, `refresh_token`, `project_id`, `dev_token`, `email`,
`login_customer_id`, `skip_customer_ids[]`; `meta` carries `token`, `account_ids[]`.

## Testing

`pytest`, offline fixtures, no network — mirroring `scripts/seo/`. Coverage that matters:

- A TSV whose columns were renamed parses to zero rows and must warn, not report `$0`.
- An earnings filename whose account number would be mistaken for the month.
- A factor of exactly `0` must be rejected, not treated as valid.
- `Unavailable` never becomes `0` in the net, at any composition.
- A warning containing `<`, `>` and `&` survives rendering.
- Truncation lands on a line boundary.
- Rounding: revenue, spend and net always subtract as displayed; a small negative never renders `$-0`.

## Out of scope

Nothing is written back. The PNL app deliberately refuses to create a revenue row for a month still
in progress and must not gain an endpoint that changes this — that refusal is what makes its revenue
table trustworthy.

## Corrections found by running it live (2026-08-10)

The design was implemented and verified against the live APIs. Four defects surfaced only
against real data, all now fixed:

1. **No hardcoded currency list.** The first implementation primed the rate table from a
   fixed list of ~10 codes. The live App Store reports carry **44** distinct currencies and a
   single month of Play sales carries **56** — so 33 were silently dropped and the App Store
   read **$3,174 instead of $3,937**, a 19% understatement that looked entirely plausible.
   Rates are now resolved on first sight, and there is no list to fall out of date.
2. **This was also why Play was excluded.** Every one of the 34 settled months contained an
   unpriceable currency, so no net factor could ever be derived.
3. **A blank proceeds currency is not USD.** Apple leaves it blank on free installs (145 of
   845 rows), always with `0.00` proceeds. Zero converts to zero from anywhere; a blank code
   on a *non-zero* amount is now an unknown rather than being booked at par.
4. **The rate table is primed in one request.** Since conversion is all-or-nothing, ~60
   independent lookups meant one transient blip on one currency could discard an entire
   source. A single USD-base request, inverted, collapses sixty chances to fail into one and
   cuts the run from ~50s to ~15s. Inversion differs from the direct per-code rate by about
   0.03%, which moves the reported net by at most a dollar.

Live at the time of writing: App Store `$3,937`, Play Store `$5,320`, Google Ads `$1,920`,
Meta Ads `$909`, Influencer `$0`, net `$6,428`. 58 non-USD currencies resolved, none
unpriceable. Google and Meta both bill in AED, so the FX path is load-bearing for spend too —
unconverted, those lines would overstate by 3.7×.

## Open items

1. Whether the existing `TELEGRAM_BOT_TOKEN` in `pnl`/`prod` belongs to a bot that is a member of
   group `-5466007383`. If it isn't, either add it to the group or point the workflow at a GitHub
   secret instead. The first run answers this — a non-member bot returns `403 bot is not a member`,
   which is a delivery failure and therefore red.
