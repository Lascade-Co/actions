#!/usr/bin/env bash
# Send the install->paid email via Resend. Prints HTTP status only (public log).
# Usage: send_resend_ip.sh <report_date> <key_suffix> ; env: RESEND_API_KEY
# The recipient is fixed here on purpose: this email goes to Cherian only.
# key_suffix is "scheduled" for cron runs and "manual-<run_id>" for tests, so a test
# never makes the next scheduled send read as "already sent".
# The payload is written once before the first attempt; retries reuse it unchanged.
set -euo pipefail
RD="$1"; SUFFIX="$2"
TO="${SHARE_TO:-cherian@lascade.com}"   # SHARE_TO: temporary one-off share, manual runs only
KEY="install-paid-$RD-$SUFFIX"
[ -f payload.json ] || jq -n --rawfile html out/email-A.html --rawfile text out/email-A.txt \
      --rawfile subject out/subject-A.txt --arg to "$TO" \
  '{from:"Lascade Product <noreply@metrics.lascade.com>", to:[$to],
    subject:($subject | rtrimstr("\n")), html:$html, text:$text}' > payload.json
for attempt in 1 2 3 4; do
  STATUS=$(curl -sS --connect-timeout 10 --max-time 60 -o resp.json -w '%{http_code}' -X POST https://api.resend.com/emails \
    -H "Authorization: Bearer $RESEND_API_KEY" -H "Idempotency-Key: $KEY" \
    -H "Content-Type: application/json" --data @payload.json) || STATUS=000
  echo "attempt $attempt: HTTP $STATUS"
  case "$STATUS" in
    200|201) echo "sent"; exit 0 ;;
    409)
      # Same key still in flight: wait and retry. A replay of a sent payload returns the
      # original 200, so every other 409 (key reused with a different payload) is a failure.
      if jq -e '.name == "concurrent_idempotent_requests"' resp.json >/dev/null 2>&1; then sleep 10; continue; fi
      echo "conflict on the send key"; exit 1 ;;
    429|5*|000) [ "$attempt" -lt 4 ] && sleep $((attempt * 15)) ;;
    *) exit 1 ;;
  esac
done
exit 1
