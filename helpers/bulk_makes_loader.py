#!/usr/bin/env python3
"""
bulk_makes_loader.py – Insert/update JSON dumps into the `makes` table.
Requires:
    pip install mysql-connector-python python-dateutil
"""

import json
import os
from pathlib import Path
from typing import Optional
from dateutil import parser as date_parser
import mysql.connector
from mysql.connector import errorcode

# ------------------------------------------------------------------------------
# 1.  DB connection
# ------------------------------------------------------------------------------
CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "root"),
    "database": os.getenv("DB_NAME", "cms_bike_backend"),
}

# ------------------------------------------------------------------------------
# 2.  Column ordering in `makes`
# ------------------------------------------------------------------------------
COLUMNS = (
    "id",             "name",           "slug",          "order",      "logoId",
    "keyFactors",     "isActive",       "createdAt",     "updatedAt",    "publishedAt",
    "tagline",        "oldCarName",     "iconImageId",   "old_cars24_id",  "metallicImageId",
    "productType",
)

# ------------------------------------------------------------------------------
# 3.  Helpers
# ------------------------------------------------------------------------------
def iso_to_mysql(iso: Optional[str]) -> Optional[str]:
    """2025-03-17T22:17:17+05:30  →  2025-03-17 16:47:17"""
    if not iso:
        return None
    dt = date_parser.isoparse(iso).astimezone(tz=None).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def normalise(js: dict) -> tuple:
    """Map snake-case JSON keys to camel/snake DB columns in COLUMNS order."""
    return (
        js.get("id"),
        js["name"],
        js["slug"],
        js.get("order"),
        js["logoid"],
        js.get("keyfactors"),
        js.get("isactive", False),
        iso_to_mysql(js.get("createdat")),
        iso_to_mysql(js.get("updatedat")),
        iso_to_mysql(js.get("publishedat")),
        js.get("tagline"),
        js.get("oldcarname"),
        js.get("iconimageid"),
        js.get("old_cars24_id"),
        js.get("metallicimageid"),
        js["producttype"],
    )


def load_json(path: Path) -> list[dict]:
    """Read path and return list[dict] irrespective of envelope shape."""
    with path.open() as f:
        blob = json.load(f)
    return blob

# ------------------------------------------------------------------------------
# 4.  Bulk insert / update with FK-safe skipping
# ------------------------------------------------------------------------------
def bulk_insert_makes(rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in COLUMNS)
    update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in COLUMNS if c != "id")

    sql = f"""
        INSERT INTO makes ({col_list})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """

    conn = mysql.connector.connect(**CONFIG)
    inserted = 0
    skipped  = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                try:
                    cur.execute(sql, row)
                    inserted += 1
                except mysql.connector.Error as e:
                    if e.errno in (
                        errorcode.ER_NO_REFERENCED_ROW_2,     # FK violation
                        errorcode.ER_NO_REFERENCED_ROW
                    ):
                        print(f"⚠️  Skipped make id={row[0]} (FK constraint): {e.msg}")
                        skipped += 1
                    else:
                        raise
        conn.commit()
    finally:
        conn.close()

    print(f"✅  Inserted/updated: {inserted}")
    print(f"⛔  Skipped (FK):      {skipped}")

# ------------------------------------------------------------------------------
# 5.  Main
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    json_path = Path("makes_dump.json")    # point to your JSON dump
    raw = load_json(json_path)
    tuples = [normalise(js) for js in raw]
    bulk_insert_makes(tuples)
