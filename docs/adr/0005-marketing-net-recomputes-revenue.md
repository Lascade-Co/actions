# The Marketing Net Action recomputes revenue instead of reading it from the PNL app

`.github/workflows/daily-marketing-net.yml` fetches App Store and Play Store month-to-date revenue
itself, duplicating parsing that already exists in `Lascade-Co/pnl`
(`pnl/services/revenue/appstore.py`, `playstore.py`). This looks like obvious duplication. It isn't.

**Why the PNL app cannot serve this figure.** Three independent blocks, each deliberate:

- `fetch_revenue` refuses a month still in progress. That refusal is the entire point of the PNL
  repo's ADR-0012 ("a month must end before revenue exists"), added after a June 2026 row was
  created on June 18 carrying a mid-month number. A read path around it would reintroduce exactly
  what that ADR removed.
- `PlayStoreAdapter` reads only `earnings/` — the finalized net, published around the middle of the
  *following* month. It has no `sales/` path and no calibration, so it has nothing to say about the
  current month at all.
- `currency.to_usd_for_month` resolves rates at `_month_end(month)`. For an in-progress month that
  is a **future date**: every lookup 404s and every non-USD line is silently dropped. See ADR-0006.

**Considered and rejected.** *A read-only `/api/mtd-revenue/` endpoint on PNL* — it would still need
a span-based path around `fetch_revenue`'s refusal, a new `sales/` reader, and an FX policy
different from the one the rest of the app uses. That is not reuse; it is a second revenue
implementation living inside the app whose stated invariant is that mid-month revenue does not
exist. *Reading the connector's ClickHouse tables* — neither table carries a currency column
(`google___google_ads` has `cost_micros` and no `currency_code`; `fb___insights` has `spend` as a
bare `String`), the Facebook `managing_system` partitions are split across live and dead names
(`d1`/`m4`/`d2` stopped updating in January 2026 while `d1a`/`m4b`/`d2c` continued), and the data
lags roughly two days.

**Consequence.** Two implementations of App Store TSV parsing now exist and may drift. They are not
expected to agree and nothing reconciles them: the Action's figure is an estimate for a live month
and is never persisted, while PNL's is the booked number for a closed month. The Action writes
nothing back, and the PNL app gains no endpoint that would let it.
