import sqlite3, sys
p = sys.argv[1] if len(sys.argv) > 1 else "wiretap.db"
c = sqlite3.connect(p)
for row in c.execute("SELECT last_seq, updated_at FROM wiretap_cursor"):
    print(f"last_seq={row[0]} updated_at={row[1]}")
