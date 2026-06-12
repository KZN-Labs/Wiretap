#!/usr/bin/env bash
# Resume + dupe/gap audit using the sqlite sink so we can query the actual
# event table after restart.
set -uo pipefail

cd "$(dirname "$0")/.."
rm -f wiretap.db events.db /tmp/wt-resume-{1,2}.{stderr,stdout}

cat > /tmp/wt-resume.toml <<EOF
[source]
endpoint = "https://fullnode.testnet.sui.io:443"
start_checkpoint = "latest"

[[watch]]
events = ["0xf5ea2b3749c65d6e56507cc35388719aadb28f9cab873696a2f8687f5c785138::oracle::*"]

[sink]
type = "sqlite"
path = "events.db"

[cursor]
path = "wiretap.db"
EOF

echo "=== run 1 (15s) ==="
timeout 15 ./target/release/wiretap watch --config /tmp/wt-resume.toml \
    >/tmp/wt-resume-1.stdout 2>/tmp/wt-resume-1.stderr
cursor1=$(python3 scripts/read-cursor.py wiretap.db | awk -F= '/last_seq/ {print $2}' | awk '{print $1}')
n1=$(python3 -c "import sqlite3;print(sqlite3.connect('events.db').execute('SELECT COUNT(*) FROM events').fetchone()[0])")
echo "cursor=$cursor1 events_table=$n1"

echo "=== sleep 15s (chain advances past cursor) ==="
sleep 15

echo "=== run 2 (30s) ==="
timeout 30 ./target/release/wiretap watch --config /tmp/wt-resume.toml \
    >/tmp/wt-resume-2.stdout 2>/tmp/wt-resume-2.stderr
cursor2=$(python3 scripts/read-cursor.py wiretap.db | awk -F= '/last_seq/ {print $2}' | awk '{print $1}')
n2=$(python3 -c "import sqlite3;print(sqlite3.connect('events.db').execute('SELECT COUNT(*) FROM events').fetchone()[0])")
echo "cursor=$cursor2 events_table=$n2"

echo
echo "=== backfill log from run 2 ==="
grep -E 'catching up before subscribe|backfilling gap' /tmp/wt-resume-2.stderr || echo "  (none)"

echo
echo "=== sqlite audit ==="
python3 - <<PY
import sqlite3
c = sqlite3.connect("events.db")
rows = c.execute("SELECT COUNT(*) total, COUNT(DISTINCT tx_digest||':'||event_seq) distinct_pk, MIN(checkpoint), MAX(checkpoint) FROM events").fetchone()
print(f"rows total           : {rows[0]}")
print(f"rows distinct (tx,seq): {rows[1]}")
print(f"checkpoint min..max  : {rows[2]} .. {rows[3]}")
print("dupes (tx_digest,event_seq):", rows[0] - rows[1])
# Sample a few cps and confirm the cursor advanced contiguously over the gap.
cp_emitting = [r[0] for r in c.execute("SELECT DISTINCT checkpoint FROM events ORDER BY checkpoint")]
gap_after_cursor1 = [cp for cp in cp_emitting if cp > $cursor1 and cp <= $cursor2]
print(f"event-emitting cps in (run1_cursor, run2_cursor] = {len(gap_after_cursor1)} (sample {gap_after_cursor1[:8]} ...)")
PY

echo
echo "=== verdict ==="
if [ "$cursor2" -gt "$cursor1" ]; then
    echo "PASS: cursor advanced $cursor1 -> $cursor2 (delta $(( cursor2 - cursor1 )))"
else
    echo "FAIL: cursor did not advance"
fi
