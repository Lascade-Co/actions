# Play Store month-to-date is calibrated sales, not gross

Google publishes no mid-month net figure. The finalized `earnings/` report — the actual payout —
does not appear until roughly the middle of the *following* month, so mid-August there is no August
earnings data whatsoever. The only in-month data is `sales/`: estimated **gross** buyer-charged
amounts, before Google's 15/30% commission, before tax, before refunds.

**Decision.** Multiply the current month's `sales/` total by a **net factor** —
`earnings ÷ sales` computed from the most recent **settled month**, meaning a month whose bucket
holds both reports. Both sides convert to USD at the single rate date (ADR-0006) before the ratio is
taken.

**Why not just sum `sales/`.** Placing an estimated-gross Play figure next to the App Store's net
proceeds overstates Play by roughly the commission, and the two revenue lines in the message would
not mean the same thing.

**The symmetry is load-bearing.** Refund rows carry a negative `Charged Amount` whose components do
not reconcile against `Item Price` + `Taxes Collected`. They are summed exactly as they stand. The
factor absorbs that discrepancy — but *only* because the identical summation rule is applied to the
factor's month and to the current month. Filtering or special-casing refunds on one side only breaks
the calibration silently.

**Fail loudly, never quietly.** If no factor can be derived — no month carries both reports, the
settled month will not convert, or the derived factor is `≤ 0` — Play is excluded and the message
says so. An uncalibrated gross figure is never reported. The guard is on the **derived value**, not
just the divisor: a zero-earnings month with non-zero sales yields a factor of exactly `0`, which
survives an `is None` check and would report Play as contributing precisely nothing.

**A parsing trap worth recording.** The month is *not* the last underscore-separated segment of an
earnings filename — the account id and sequence number follow it, so `split("_")[-1][:6]` extracts
the account number. Use a regex on `_(\d{6})`. This fails silently: zero overlapping months are
found, no factor is derived, and Play is excluded forever with no error.
