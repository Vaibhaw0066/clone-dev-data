#!/usr/bin/env python3

import json
import os
from pathlib import Path
from typing import Optional
from dateutil import parser as date_parser
import mysql.connector

# DB config (customize or load from ENV)
CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "root"),
    "database": os.getenv("DB_NAME", "cms_bike_backend")
}

# DB columns
COLUMNS = ("id", "name", "isActive", "createdAt", "updatedAt", "parentId", "tenantId")

def iso_to_mysql(iso_str: Optional[str]) -> Optional[str]:
    if not iso_str:
        return None
    dt = date_parser.isoparse(iso_str).astimezone(tz=None).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def normalise_row(js: dict) -> tuple:
    return (
        js["id"],
        js["name"],
        js.get("isactive", True),
        iso_to_mysql(js.get("createdat")),
        iso_to_mysql(js.get("updatedat")),
        js.get("parentid"),
        js["tenantid"]
    )

def load_json(path: Path) -> list[dict]:
    with path.open() as f:
        blob = json.load(f)
    return blob["data"] if "data" in blob else blob

def bulk_insert_folders(rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in COLUMNS)
    update_clause = ", ".join(
        f"`{c}` = VALUES(`{c}`)" for c in COLUMNS if c != "id"
    )

    sql = f"""
        INSERT INTO media_folder ({col_list})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """

    conn = mysql.connector.connect(**CONFIG)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        print(f"✅ Inserted/updated {len(rows)} rows into `media_folder`.")
    except mysql.connector.Error as err:
        print("❌ DB error:", err.msg)
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    json_path = Path("media_folder_dump.json")  # your JSON file
    raw_rows = load_json(json_path)
    tuples = [normalise_row(js) for js in raw_rows]
    bulk_insert_folders(tuples)
