#!/usr/bin/env bash
# Send one ad-spend variant via Resend. Prints the HTTP status only (public log).
# 409 = Resend already holds this Idempotency-Key = already sent = success.
# Usage: send_resend.sh <A|B> <ad_date> ; env: RESEND_API_KEY
set -euo pipefail
V="$1"; AD="$2"
jq -n --rawfile html "out/email-$V.html" --rawfile text "out/email-$V.txt" \
      --rawfile subject "out/email-$V.subject" \
      --arg to "${MAIL_TO:-cherian@lascade.com}" \
  '{from:"noreply@metrics.lascade.com", to:[$to],
    subject:$subject, html:$html, text:$text}' > "payload-$V.json"
STATUS=$(curl -sS -o /dev/null -w '%{http_code}' -X POST https://api.resend.com/emails \
  -H "Authorization: Bearer $RESEND_API_KEY" \
  -H "Idempotency-Key: ad-spend-$V-$AD" \
  -H "Content-Type: application/json" --data @"payload-$V.json")
echo "variant $V: HTTP $STATUS"
case "$STATUS" in
  200|201) echo "variant $V: sent" ;;
  409) echo "variant $V: already sent" ;;
  *) exit 1 ;;
esac
