#!/usr/bin/env bash
# 30s live capture against Sui testnet. Verifies:
#   - ordered (strictly non-decreasing) checkpoint sequence numbers
#   - gapless (no missed checkpoints in the captured range)
#   - lag-vs-tip progress log fires
set -uo pipefail

cd "$(dirname "$0")/.."
rm -f wiretap.db events.db /tmp/wt-events.ndjson /tmp/wt-stderr.log
timeout 30 ./target/release/wiretap watch \
    --endpoint https://fullnode.testnet.sui.io:443 \
    --sink stdout \
    >/tmp/wt-events.ndjson 2>/tmp/wt-stderr.log

echo "=== events captured ==="
wc -l /tmp/wt-events.ndjson

echo "=== checkpoint ordering ==="
jq -r .checkpoint /tmp/wt-events.ndjson > /tmp/wt-cps.txt
distinct=$(sort -un /tmp/wt-cps.txt | wc -l)
first=$(sort -un /tmp/wt-cps.txt | head -1)
last=$(sort -un /tmp/wt-cps.txt | tail -1)
expected=$(( last - first + 1 ))
echo "first=$first last=$last distinct=$distinct expected=$expected"
if [ "$distinct" = "$expected" ]; then
    echo "PASS: gapless ($distinct == $expected)"
else
    echo "FAIL: gap of $(( expected - distinct )) checkpoints"
    sort -un /tmp/wt-cps.txt | awk -v p=0 '{ if (p && $1 != p+1) print "  missing between", p, "and", $1; p=$1 }'
fi

echo "=== ordering (event order = checkpoint order) ==="
# Within stdout sink, events are emitted in the same order they arrive — so the
# checkpoint column should be monotone non-decreasing in the raw NDJSON.
violations=$(awk -v prev=0 '{ if ($1+0 < prev) print NR; prev=$1+0 }' /tmp/wt-cps.txt | wc -l)
echo "out-of-order rows: $violations"

echo "=== progress log ==="
grep -E 'wiretap: progress' /tmp/wt-stderr.log | head -5

echo "=== last stderr lines ==="
tail -8 /tmp/wt-stderr.log
