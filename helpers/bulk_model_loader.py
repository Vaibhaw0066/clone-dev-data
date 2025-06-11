#!/usr/bin/env python3
"""
bulk_models_loader.py – Insert/update JSON dumps into the `model` table.

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
# 1.  DB connection config
# ------------------------------------------------------------------------------
CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", "root"),
    "database": os.getenv("DB_NAME", "cms_bike_backend"),
}

# ------------------------------------------------------------------------------
# 2.  Column list in DB order (match MySQL `DESCRIBE model`)
# ------------------------------------------------------------------------------
COLUMNS = (
    "id", "name", "slug", "makeId", "launchStatus", "launchedDate",
    "discontinuedDate", "estimatedLaunchedDate", "featureVideoURL",
    "featureVideoStartTime", "featureVideoEndTime", "keyHighlights", "isActive",
    "isPopular", "createdAt", "updatedAt", "nextModelId", "featureImageId",
    "bikeImageId", "bikeImageWithBackgroundId", "bikeGifId", "modelType",
    "maxPriceInLakh", "minPriceInLakh", "brochureId", "featureImageForMobileId",
    "compareBikeImageId", "overallUserReviewSummary", "overallUserReviewPros",
    "overallUserReviewCons", "overallExpertReviewSummary", "overallExpertReviewPros",
    "overallExpertReviewCons", "overallSummary", "overallPros", "overallCons",
    "previewVideoId", "productType", "subType", "overallSocialReviewSummary",
    "overallSocialReviewPros", "overallSocialReviewCons", "old_cars24_id",
    "avgUserReviewsRating", "avgExpertReviewsRating", "avgSocialReviewsRating",
    "totalWeightedUserReviewsHelpfulness", "totalWeightedSocialReviewsHelpfulness",
    "totalWeightedUserReviewsHelpfulnessBuy", "totalWeightedUserReviewsHelpfulnessSell",
    "totalWeightedUserReviewsHelpfulnessNeutral",
    "totalWeightedSocialReviewsHelpfulnessBuy",
    "totalWeightedSocialReviewsHelpfulnessSell",
    "totalWeightedSocialReviewsHelpfulnessNeutral", "overallScore", "oldCarName",
    "totalWeightedHelpfulness", "totalWeightedUserReviewsHelpfulnessBuyPercentage",
    "totalWeightedUserReviewsHelpfulnessSellPercentage",
    "totalWeightedUserReviewsHelpfulnessNeutralPercentage",
    "totalWeightedSocialReviewsHelpfulnessBuyPercentage",
    "totalWeightedSocialReviewsHelpfulnessSellPercentage",
    "totalWeightedSocialReviewsHelpfulnessNeutralPercentage", "overallRating",
    "overallBuyPercentage", "overallSellPercentage", "overallNeutralPercentage",
)

# ------------------------------------------------------------------------------
# 3.  Utility functions
# ------------------------------------------------------------------------------
def iso_to_mysql(iso_str):
    try:
        dt = date_parser.isoparse(iso_str)
        return dt.astimezone(tz=None).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return iso_str  # Return original if parsing fails


def normalise(js):
    js = js.copy()

    date_fields = [
        'createdat', 'updatedat', 'launcheddate',
        'discontinueddate', 'estimatedlauncheddate'
    ]

    for field in date_fields:
        if field in js and js[field]:
            js[field] = iso_to_mysql(js[field])

    if 'keyhighlights' in js and js['keyhighlights']:
        try:
            js['keyhighlights'] = json.loads(js['keyhighlights'])  # Parse to list
            js['keyhighlights'] = json.dumps(js['keyhighlights'])  # Convert back to JSON string
        except Exception:
            pass  # Keep as-is if invalid

    return js


def load_json(path: Path) -> list[dict]:
    with path.open() as f:
        blob = json.load(f)
    return blob

# ------------------------------------------------------------------------------
# 4.  Bulk insert/update
# ------------------------------------------------------------------------------
def bulk_insert_models(rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    col_list = ", ".join(f"`{c}`" for c in COLUMNS)
    update_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in COLUMNS if c != "id")

    sql = f"""
        INSERT INTO model ({col_list})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """

    conn = mysql.connector.connect(**CONFIG)
    inserted = 0
    skipped = 0
    try:
        with conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            for row in rows:
                try:

                    cur.execute(sql, row)
                    inserted += 1
                except mysql.connector.Error as e:
                    if e.errno in (
                            errorcode.ER_NO_REFERENCED_ROW_2,
                            errorcode.ER_NO_REFERENCED_ROW,
                    ):
                        print(f"⚠️  Skipped model id={row[0]} (FK constraint): {e.msg}")
                        skipped += 1
                    elif e.errno == errorcode.ER_DUP_ENTRY:
                        print(f"⚠️  Skipped model id={row[0]} (Duplicate key): {e.msg}")
                        skipped += 1
                    else:
                        raise
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()
    finally:
        conn.close()

    print(f"✅  Inserted/updated: {inserted}")
    print(f"⛔  Skipped (FK):      {skipped}")

# ------------------------------------------------------------------------------
# 5.  Entry Point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    json_path = Path("model_dump.json")   # <- your JSON file path
    raw = load_json(json_path)
    tuples = [tuple(normalise(js).get(col.lower()) for col in COLUMNS) for js in raw]
    bulk_insert_models(tuples)
