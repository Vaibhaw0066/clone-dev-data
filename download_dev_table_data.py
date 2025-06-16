
import os
import json
import requests
import time
import logging

# === CONFIGURATION ===
API_URL = "https://cms-bike-backend.qac24svc.dev/api/v1/misc/execute"
AUTH_TOKEN = "<your_actual_token_here>"
LOG_FILE = "db_dump_log.log"
TARGET_SCHEMA = "cms_bike_backend_qa"
OUTPUT_DIR = "dev-db-data"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AUTH_TOKEN}"
}

# === SETUP ===
def setup_output_directory():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

# === LOGGING SETUP ===
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filemode='a'
)


# === API CALLS ===
def execute_query(query, timeout=30):
    response = requests.post(API_URL, headers=HEADERS, json={"query": query}, timeout=timeout)
    response.raise_for_status()
    return response.json()

# === TABLE OPERATIONS ===
def get_table_names():
    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{TARGET_SCHEMA}' AND table_type = 'BASE TABLE';
    """
    result = execute_query(query)
    if result.get("data", {}).get("success"):
        return [row["table_name"] for row in result["data"]["data"]]
    raise Exception("Failed to fetch table list")

def fetch_actual_columns(table_name):
    query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}' AND TABLE_NAME = '{table_name}';
    """
    try:
        result = execute_query(query, timeout=20)
        rows = result.get("data", {}).get("data", [])
        if not rows:
            raise ValueError(f"❌ Schema query returned empty for table: {table_name}")
        return [row.get("COLUMN_NAME") or row.get("column_name") for row in rows if row.get("COLUMN_NAME") or row.get("column_name")]
    except Exception as e:
        logging.error(f"❗ Failed to fetch columns for {table_name}: {e}")
        return []

def normalize_row_keys(rows, actual_columns):
    normalized = []
    lower_col_map = {col.lower(): col for col in actual_columns}
    for row in rows:
        new_row = {lower_col_map[key.lower()]: value for key, value in row.items() if key.lower() in lower_col_map}
        normalized.append(new_row)
    return normalized

def dump_table_to_json(table_name):
    query = f"SELECT * FROM {table_name};"
    logging.info(f"⤵️ Downloading data from : {table_name}")
    print(f"⤵️ Downloading started for : {table_name}", end=" ")
    try:
        result = execute_query(query)
        if result.get("data", {}).get("success"):
            rows = result["data"]["data"]
            if not rows:
                logging.warning(f"⚠️  Empty table: {table_name}")
                return
            actual_columns = fetch_actual_columns(table_name)
            if not actual_columns:
                logging.warning(f"⚠️ Skipping {table_name} due to missing schema info.")
                return
            normalized_rows = normalize_row_keys(rows, actual_columns)
            out_file = os.path.join(OUTPUT_DIR, f"{table_name}_dump.json")
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(normalized_rows, f, indent=2, ensure_ascii=False)
            logging.info(f"✅ Saved: {out_file}")
            print(f"   Downloaded : {table_name}", end="  ✅️\n")

        else:
            error_msg = result.get("error", {}).get("message", "Unknown error")
            print(f"   Downlaod failed : {table_name},  API Error: {error_msg} : ", end="  ❌\n")
            logging.error(f"❌ API Error: {error_msg}")
    except Exception as e:
        logging.error(f"❗ Failed to query {table_name}: {e}")
        print(f"❗ Failed to download {table_name}, ERROR: {e}", end="  ❌\n")

# === MAIN EXECUTION ===
def main():
    setup_output_directory()
    try:
        tables = get_table_names()
        logging.info(f"🔍 Found {len(tables)} tables in schema '{TARGET_SCHEMA}'")
        for table in tables:
            dump_table_to_json(table)
            time.sleep(0.3)
    except KeyboardInterrupt:
        logging.warning("🛑 Interrupted by user.")
    except Exception as e:
        logging.error(f"❗ Fatal error: {e}")

# === EXTERNAL HOOK ===
def download_db_data_from_dev(bearer_token, log_file):
    global AUTH_TOKEN, HEADERS, LOG_FILE
    if bearer_token:
        AUTH_TOKEN = bearer_token
        LOG_FILE = log_file
        HEADERS["Authorization"] = f"Bearer {AUTH_TOKEN}"
    print("✅ Downloading started. For details check `db_clone_log` file. ")
    main()
    print("✅ Tables data downloaded successfully.")
# === SCRIPT ENTRY ===
if __name__ == "__main__":
    main()
