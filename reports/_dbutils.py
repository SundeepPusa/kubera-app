# FILE: reports/_dbutils.py
import csv
import json
import datetime as dt
from io import StringIO
import psycopg2
import psycopg2.extras

def day_replace_insert(conn, *, table: str, trade_date: dt.date, rows, columns, analyze: bool = True):
    """
    Hard-replace all rows for `trade_date` in `table` with the provided `rows`.

    - `rows`: iterable[dict] -> keys must match names in `columns` (order respected)
    - `columns`: list[str]  -> destination column order for COPY/INSERT
    """
    with conn:
        with conn.cursor() as cur:
            # 1) take a short-lived advisory lock per table+date to avoid races
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s) ^ hashtext(%s));", (table, str(trade_date)))

            # 2) delete existing rows for the day
            cur.execute(f"DELETE FROM {table} WHERE trade_date = %s;", (trade_date,))

            # 3) COPY into a temp table, then INSERT → fast + safe
            collist = ", ".join(columns)
            cur.execute(f"""
                CREATE TEMP TABLE _tmp_day_load({collist}) ON COMMIT DROP;
            """)

            # Build CSV in-memory for COPY
            buf = StringIO()
            w = csv.writer(buf, lineterminator="\n")
            for r in rows:
                w.writerow([r.get(c) for c in columns])
            buf.seek(0)

            # COPY then INSERT
            cur.copy_expert(f"COPY _tmp_day_load({collist}) FROM STDIN WITH CSV", buf)
            cur.execute(f"INSERT INTO {table}({collist}) SELECT {collist} FROM _tmp_day_load;")

            # 4) optional analyze for planner stats
            if analyze:
                cur.execute(f"ANALYZE {table};")

    return True
