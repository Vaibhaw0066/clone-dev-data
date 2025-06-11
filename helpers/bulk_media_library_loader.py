#!/usr/bin/env python3

import json
import os
from pathlib import Path
from typing import Optional
from dateutil import parser as date_parser
import mysql.connector

# ------------------------------------------------------------------------------
# DB connection config – pull from ENV or default to localhost
# ------------------------------------------------------------------------------
CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "root"),
    "database": os.getenv("DB_NAME", "cms_bike_backend")
}

# ------------------------------------------------------------------------------
# Columns in the media_library table
# ------------------------------------------------------------------------------
COLUMNS = (
    "id", "url", "alternativeText", "name", "caption",
    "isActive", "tenantId", "createdAt", "updatedAt", "folderId",
    "category", "isHeaderImage", "colorSlug"
)

def iso_to_mysql(iso_str: Optional[str]) -> Optional[str]:
    if not iso_str:
        return None
    dt = date_parser.isoparse(iso_str).astimezone(tz=None).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ------------------------------------------------------------------------------
# Transform a single JSON row to match the DB column order
# ------------------------------------------------------------------------------
def normalise_row(js: dict) -> tuple:
    return (
        js.get("id"),
        js["url"],
        js.get("alternativetext"),
        js.get("name"),
        js.get("caption"),
        js.get("isactive", True),
        js["tenantid"],
        iso_to_mysql(js.get("createdat")),
        iso_to_mysql(js.get("updatedat")),
        js["folderid"],
        js.get("category"),
        js.get("isheaderimage", False),
        js.get("colorslug"),
    )

# ------------------------------------------------------------------------------
# Load JSON data from a file
# ------------------------------------------------------------------------------
def load_json(path: Path) -> list[dict]:
    with path.open() as f:
        blob = json.load(f)
    return blob

# ------------------------------------------------------------------------------
# Bulk insert into media_library
# ------------------------------------------------------------------------------
def bulk_insert_media(rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in COLUMNS)
    update_clause = ", ".join(
        f"`{c}` = VALUES(`{c}`)" for c in COLUMNS if c != "id"
    )

    sql = f"""
        INSERT INTO media_library ({col_list})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """

    conn = mysql.connector.connect(**CONFIG)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        print(f"✅ Inserted/updated {len(rows)} rows into `media_library`.")
    except mysql.connector.Error as err:
        print("❌ DB error:", err.msg)
        conn.rollback()
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    json_path = Path("media_library_dump.json")  # JSON input
    raw_rows = load_json(json_path)
    tuples = [normalise_row(js) for js in raw_rows]
    bulk_insert_media(tuples)
