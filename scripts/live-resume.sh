#!/usr/bin/env bash
# Resume test:
#   1. Run watch for ~10s, kill it.
#   2. Record cursor state.
#   3. Wait ~12s so the testnet tip advances past the recorded cursor.
#   4. Restart watch; expect the "catching up before subscribe" INFO log
#      from LedgerService.GetCheckpoint covering the missed range, and the
#      cursor to advance past the recorded value within ~10s.
set -u

cd "$(dirname "$0")/.."
rm -f wiretap.db events.db /tmp/wt-resume-{1,2}.{stdout,stderr,events}

echo "=== run 1 (10s) ==="
timeout 10 ./target/release/wiretap watch \
    --endpoint https://fullnode.testnet.sui.io:443 \
    --sink stdout \
    >/tmp/wt-resume-1.stdout 2>/tmp/wt-resume-1.stderr
cursor1=$(python3 scripts/read-cursor.py wiretap.db | awk -F= '/last_seq/ {print $2}' | awk '{print $1}')
echo "cursor after run 1: $cursor1"

echo "=== sleep 12s so chain advances past cursor ==="
sleep 12

echo "=== run 2 (30s) ==="
timeout 30 ./target/release/wiretap watch \
    --endpoint https://fullnode.testnet.sui.io:443 \
    --sink stdout \
    >/tmp/wt-resume-2.stdout 2>/tmp/wt-resume-2.stderr
cursor2=$(python3 scripts/read-cursor.py wiretap.db | awk -F= '/last_seq/ {print $2}' | awk '{print $1}')
echo "cursor after run 2: $cursor2"

echo
echo "=== catch-up log line(s) from run 2 ==="
grep -E 'catching up before subscribe|backfilling gap' /tmp/wt-resume-2.stderr || echo "  (none)"

echo
echo "=== verdict ==="
if [ "$cursor2" -gt "$cursor1" ]; then
    echo "PASS: cursor advanced from $cursor1 → $cursor2 (delta $(( cursor2 - cursor1 )))"
else
    echo "FAIL: cursor did not advance ($cursor1 → $cursor2)"
fi
