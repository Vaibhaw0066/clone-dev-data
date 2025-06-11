#!/usr/bin/env python3
"""
bulk_price_loader.py – Insert/update large JSON blobs into the `price` table.
Install dependency first:
    pip install mysql-connector-python python-dateutil
"""

import json
from pathlib import Path
from typing import Optional
from dateutil import parser as date_parser  # handles 2025-05-30T21:10:41+05:30
import mysql.connector
from mysql.connector import errorcode

# ------------------------------------------------------------------------------
# 1.  Connection config – put real values (or export as ENV vars)
# ------------------------------------------------------------------------------

CONFIG = {
    "host": "127.0.0.1",          # or "localhost"
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "cms_bike_backend"
}
# ------------------------------------------------------------------------------
# 2.  Column name mapping – JSON key -> DB column
# ------------------------------------------------------------------------------

KEY_MAP = {
    "roadtax":        "roadTax",
    "tcs":            "tcs",
    "lifetax":        "lifeTax",
    "statutoryfees":  "statutoryFees",
    "regcharges":     "regCharges",
    "greentax":       "greenTax",
    "roadsafetytax":  "roadSafetyTax",
    "cowcess":        "cowCess",
    "insurance":      "insurance",
    "onroadprice":    "onRoadPrice",
    "modelid":        "modelId",
    "variantid":      "variantId",
    "stateid":        "stateId",
    "producttype":    "productType",
    # createdAt / updatedAt are handled separately
}

COLUMNS = (
    "roadTax", "tcs", "lifeTax", "statutoryFees", "regCharges",
    "greenTax", "roadSafetyTax", "cowCess", "insurance", "onRoadPrice",
    "modelId", "variantId", "stateId", "productType",
    "createdAt", "updatedAt"
)

# ------------------------------------------------------------------------------
# 3.  Load JSON  (edit path or read from API)
# ------------------------------------------------------------------------------

def load_json(path: Path) -> list[dict]:
    with path.open() as f:
        blob = json.load(f)
    return blob   # adjust if your envelope differs


# ------------------------------------------------------------------------------
# 4.  Normalise one JSON row into a tuple matching `COLUMNS`
# ------------------------------------------------------------------------------

def normalise_row(js: dict) -> tuple:
    row = {}
    # map & copy
    for js_key, col in KEY_MAP.items():
        row[col] = js.get(js_key)

    # timestamps – ISO/Zoned -> naive UTC string (MySQL TIMESTAMP expects yyyy-MM-dd HH:mm:ss)
    row["createdAt"] = iso_to_mysql(js.get("createdat"))
    row["updatedAt"] = iso_to_mysql(js.get("updatedat"))

    # column order
    return tuple(row[col] for col in COLUMNS)


def iso_to_mysql(iso_str: Optional[str]) -> Optional[str]:
    if not iso_str:
        return None
    # convert to aware datetime, then to UTC naïve
    dt = date_parser.isoparse(iso_str).astimezone(tz=None).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------------------
# 5.  Bulk insert with ON DUPLICATE KEY UPDATE
# ------------------------------------------------------------------------------

def bulk_insert_price(rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in COLUMNS)
    update_clause = ", ".join(
        f"`{c}` = VALUES(`{c}`)" for c in COLUMNS
        if c not in ("variantId", "stateId", "modelId")
    )

    sql = f"""
        INSERT INTO price ({col_list})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """

    conn = mysql.connector.connect(**CONFIG)
    skipped = 0
    inserted = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                try:
                    cur.execute(sql, row)
                    inserted += 1
                except mysql.connector.Error as e:
                    if e.errno == errorcode.ER_NO_REFERENCED_ROW_2:
                        print(f"⚠️ Skipped row (FK constraint): modelId={row[COLUMNS.index('modelId')]}, stateId={row[COLUMNS.index('stateId')]}")
                        skipped += 1
                    else:
                        print(f"❌ Unhandled DB error: {e}")
        conn.commit()
    finally:
        conn.close()
        print(f"✅ Inserted: {inserted}")
        print(f"⛔ Skipped (FK violations): {skipped}")


# ------------------------------------------------------------------------------
# 6.  Main driver
# ------------------------------------------------------------------------------

def clone_price_table(table_dump_data):
    json_path = Path(table_dump_data)   # ✏️  change to your file
    raw_rows  = load_json(json_path)
    tuples    = [normalise_row(js) for js in raw_rows]
    bulk_insert_price(tuples)


# export DB_USER=root
# export DB_PASS=root
# export DB_NAME=cms_bike_backend
# export DB_HOST=0.0.0.0
# export DB_PORT=3306

# python3.10 -m sql_env .sql_env