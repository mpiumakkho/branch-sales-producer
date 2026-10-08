#!/usr/bin/env bash
# Prints a demo branch's send state from its MongoDB (collection sync_state), one line per (type, date, revision).
#   demo-branches/sync-state.sh BR0001             # current status
#   demo-branches/sync-state.sh BR0001 --history   # plus every event: send attempts and HQ receipts
set -euo pipefail
branch=${1:?usage: $0 <branchCode> [--history]}
history=${2:-}
cd "$(dirname "$0")/.."
# The password is read inside the container, from the MongoDB service's own environment
docker compose -f docker-compose.yml -f "demo-branches/$branch.compose.yaml" exec -T -e SHOW_HISTORY="$history" mongodb sh -c \
  'mongosh --quiet -u branch_sales -p "$MONGO_PASSWORD" --authenticationDatabase branch_sales branch_sales --eval "
const bkk = d => d ? d.toLocaleString(\"sv-SE\", { timeZone: \"Asia/Bangkok\" }) : \"\";
db.sync_state.find().sort({ _id: 1 }).forEach(s => {
  const note = s.status === \"HQ_REJECTED\" ? s.hqDetail : (s.lastError || (s.hqOutcome ? s.hqOutcome + \" stored revision \" + s.hqStoredRevision : \"\"));
  print([(s.type === \"DAILY_RETURN\" ? \"RETURN \" : \"SALES  \") + s.date, \"r\" + s.revision, s.status.padEnd(11), \"attempts=\" + s.attempts, \"offsets=\" + (s.sentOffsets || []).join(\",\"), note].join(\"  \"));
  if (process.env.SHOW_HISTORY === \"--history\") {
    s.history.forEach(e => print(\"    \" + [bkk(e.at), e.result.padEnd(12), e.offset !== undefined ? \"offset=\" + e.offset : \"\", e.error || e.rejectReason || \"\"].join(\"  \")));
  }
});"'
