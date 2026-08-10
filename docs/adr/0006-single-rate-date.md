# One FX rate date per Marketing Net run

Every non-USD amount in a run converts at **yesterday's** rates — one rate table, fetched once, used
for current-month revenue and for *both sides of the Play Store net factor*, including a settled
month that may be a year old.

**Why not month-end.** The month-end of an in-progress month is a date in the future. Every rate
source 404s for it, the lookup returns nothing, and each non-USD row is silently dropped — leaving a
perfectly plausible figure assembled from the USD portion alone. The PNL app's `currency.py` has
precisely this shape (`_month_rate_to_usd` → `_month_end`) and is correct there only because it runs
exclusively on closed months. Yesterday is the most recent date a rate reliably exists for, and it
is the same boundary the App Store window already stops at.

**Why the factor's month too.** The net factor is a ratio of two sums drawn from one settled month.
Converting numerator and denominator at the same rates makes it a pure net/gross ratio — the rate
choice cancels out. Converting each at its own month's rates leaves the factor carrying however much
FX has drifted since, which has nothing to do with Google's commission. The requirement that an
identical summation rule apply to both sides of the factor extends to FX for the same reason: the
symmetry is what makes the factor mean anything.

**Consequence.** The figure is stated in yesterday's money rather than in the money of the days the
sales actually occurred. For a number read for its magnitude, its trend and its sign, that is the
right trade. For anything needing per-day valuation it is not — and this figure is never booked, so
nothing downstream depends on that precision. Negative rate lookups are cached only briefly (~10
minutes, never longer than the run interval) so that a short FX outage cannot pin the figure for
days.
