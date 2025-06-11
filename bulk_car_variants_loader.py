#!/usr/bin/env python3
"""
Bulk-insert / update JSON array into `variant_car`.
  • Requires: pip install mysql-connector-python python-dateutil
  • Place your JSON file (array of objects) alongside this script, e.g. variants_dump.json
"""

import json
import os
from pathlib import Path
from typing import *

from dateutil import parser as dt_parser
import mysql.connector
from mysql.connector import errorcode

# ──────────────────────────────────────────────────────────────────────────────
# 1️⃣  DB config  – override with ENV vars if you like
# ──────────────────────────────────────────────────────────────────────────────
CONFIG = {
    "host":     os.getenv("DB_HOST", "127.0.0.1"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "root"),
    "database": os.getenv("DB_NAME", "cms_bike_backend"),
}
TABLE_NAME = "variant_car"
JSON_FILE   = "car_variants_dump.json"   # your file here
DISABLE_FK  = True                   # set False in prod if you want FK enforced


def iso_to_mysql(val: Optional[str]) -> Optional[str]:
    """Convert ISO 8601 → 'YYYY-MM-DD HH:MM:SS'.  Return unchanged on failure."""
    if not val:
        return None
    try:
        dt = dt_parser.isoparse(val)
        return dt.astimezone(tz=None).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:  # ValueError, TypeError
        return val  # leave plain time / invalid strings untouched

def load_json(path: Union[str, Path]) -> List[dict]:
    with Path(path).open() as fh:
        blob = json.load(fh)
    # accept either raw array or envelope {"data": [...] }
    return blob


def fetch_columns(cur, schema: str, table: str) -> list[str]:
    cur.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    return [row[0] for row in cur.fetchall()]


def normalise_row(js: dict, columns: list[str]) -> tuple:
    """Return values tuple aligned to columns."""
    row = []
    for col in columns:
        key = col.lower()          # json keys are lowercase
        val = js.get(key)

        # datetime columns we care about
        if col in ("createdAt", "updatedAt", "publishedAt"):
            val = iso_to_mysql(val)

        # convert any list/dict back to JSON string for JSON/TEXT columns (e.g. colorDetails)
        if isinstance(val, (list, dict)):
            val = json.dumps(val, ensure_ascii=False)

        row.append(val)
    return tuple(row)


# ──────────────────────────────────────────────────────────────────────────────
# 3️⃣  Main bulk loader
# ──────────────────────────────────────────────────────────────────────────────
def main() -> None:
    variants = load_json(JSON_FILE)

    conn = mysql.connector.connect(**CONFIG)
    cur  = conn.cursor()

    columns = fetch_columns(cur, CONFIG["database"], TABLE_NAME)
    placeholders = ", ".join(["%s"] * len(columns))
    col_list     = ", ".join(f"`{c}`" for c in columns)
    update_cols  = [c for c in columns if c != "id"]          # every col except PK
    upd_clause   = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)

    sql = f"""INSERT INTO {TABLE_NAME} ({col_list})
              VALUES ({placeholders})
              ON DUPLICATE KEY UPDATE {upd_clause}"""

    inserted = skipped_fk = skipped_dup = 0

    try:
        if DISABLE_FK:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")

        for js in variants:
            data = normalise_row(js, columns)
            try:
                cur.execute(sql, data)
                inserted += 1
            except mysql.connector.Error as e:
                if e.errno in (errorcode.ER_NO_REFERENCED_ROW, errorcode.ER_NO_REFERENCED_ROW_2):
                    skipped_fk += 1
                elif e.errno == errorcode.ER_DUP_ENTRY:
                    skipped_dup += 1
                else:
                    raise

        if DISABLE_FK:
            cur.execute("SET FOREIGN_KEY_CHECKS=1")

        conn.commit()
    finally:
        cur.close()
        conn.close()

    print(f"✅ inserted / updated : {inserted}")
    print(f"⛔ skipped (FK)       : {skipped_fk}")
    print(f"⛔ skipped (duplicate): {skipped_dup}")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
