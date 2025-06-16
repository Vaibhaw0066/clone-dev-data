
import os
import json
import logging
import pymysql
from collections import defaultdict, deque

# ---- CONFIG ----
CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "cms_bike_backend"
}
DUMP_DIR = "dev-db-data"

# ---- DB CONNECTION ----
def get_connection():
    return pymysql.connect(
        host=CONFIG["host"],
        port=CONFIG["port"],
        user=CONFIG["user"],
        password=CONFIG["password"],
        database=CONFIG["database"],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

# ---- UTILITY METHODS ----
def load_dump_data(table):
    file_path = os.path.join(DUMP_DIR, f"{table}_dump.json")
    if not os.path.exists(file_path):
        file_path = os.path.join(DUMP_DIR, f"{table}.json")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"❌ Failed to load dump for {table}: {e}")
        return []

def insert_sql(cursor, table, row):
    columns = row.keys()
    placeholders = ", ".join(["%s"] * len(columns))
    quoted_columns = ", ".join([f"`{col}`" for col in columns])
    sql = f"INSERT INTO `{table}` ({quoted_columns}) VALUES ({placeholders})"
    values = tuple(row[col] for col in columns)
    try:
        cursor.execute(sql, values)
    except pymysql.MySQLError as e:
        logging.error(f"❌ Insert failed for {table}: {e} — Row: {row}")

def insert_data(table, data):
    if not data:
        logging.warning(f"⚠️  No data found for table: {table}")
        return
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for i in range(0, len(data), 20):
                    batch = data[i:i + 20]
                    for row in batch:
                        insert_sql(cursor, table, row)
    except pymysql.MySQLError as e:
        logging.error(f"❌ Unexpected DB failure for {table}: {e}")

def insert_model_in_two_passes(model_data):
    if not model_data:
        logging.warning("⚠️ No model data to insert.")
        return
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                for row in model_data:
                    temp = row.copy()
                    temp["nextModelId"] = None
                    insert_sql(cursor, 'model', temp)
                for row in model_data:
                    if row.get("nextModelId"):
                        try:
                            cursor.execute(
                                "UPDATE model SET nextModelId=%s WHERE id=%s",
                                (row["nextModelId"], row["id"])
                            )
                        except pymysql.MySQLError as e:
                            logging.error(f"❌ Failed to update nextModelId in model (id={row['id']}): {e}")
    except pymysql.MySQLError as e:
        logging.error(f"❌ DB failure during model 2-pass insert: {e}")

# ---- RESTORE ORDERS ----
INSERT_ORDER = [
    'media_folder',
    'media_library',
    'state',
    'city',
    'area',
    'dealer',
    'partner',
    'makes',
    'tag',
    'model',
    'variant',
    'model_video',
    'model_video_category',
    'model_video_header',
    'celebrity',
    'model_celebrity',
    'model_color_image',
    'model_image',
    'model_tags',
    'awards',
    'model_awards',
    'model_spec',
    'model_spec_image',
    'widget_data',
    'expert_review',
    'fun_fact',
    'modelGncap',
    'thought',
    'variant_tags',
    'variant_car',
    'price',
    'dealer_makes',
    'dealer_partner',
    'make_tags',
    'image_tags',
    'user_review',
    'user_review_image',
    'social_review',
    'social_review_comment',
    'leads',
    'schedule_lead',
    'partner_lead',
    'other_popular_makes',
    'state_wise_make_registration',
    'review_report',
    'monthly_sales'
]

DELETE_ORDER = list(reversed(INSERT_ORDER))

# ---- VERIFY ORDER ----
def verify_insert_order(order):
    position = {table: i for i, table in enumerate(order)}
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT TABLE_NAME, REFERENCED_TABLE_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL
            """, (CONFIG["database"],))
            errors = []
            for row in cursor.fetchall():
                dependent = row["TABLE_NAME"]
                referenced = row["REFERENCED_TABLE_NAME"]
                if dependent in position and referenced in position:
                    if position[dependent] < position[referenced]:
                        errors.append(f"❌ {dependent} comes before its dependency {referenced}")
            if errors:
                print("🚨 Invalid INSERT_ORDER detected:")
                for err in errors:
                    print(err)
            else:
                print("✅ INSERT_ORDER is valid and FK-safe.")

# ---- DELETE AND INSERT ----
def delete_all_data_in_order():
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            for table in DELETE_ORDER:
                try:
                    logging.info(f"🚮 Deleting from table: {table}")
                    cursor.execute(f"DELETE FROM `{table}`")
                except Exception as e:
                    logging.error(f"❌ Failed to delete from {table}: {e}")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

def insert_all_data_in_order():
    for table in INSERT_ORDER:
        data = load_dump_data(table)
        logging.info(f"📥 Inserting into table: {table}")
        if table == "model":
            insert_model_in_two_passes(data)
        else:
            insert_data(table, data)

# ---- MAIN ENTRY ----
def clean_and_restore(LOG_FILE):
    logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filemode='a'  # Always append
)

    verify_insert_order(INSERT_ORDER)
    delete_all_data_in_order()
    insert_all_data_in_order()
    print("✅ Database restoration completed successfully.")

if __name__ == "__main__":
    clean_and_restore()