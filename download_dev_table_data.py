import os
import json
import requests
import time

# === CONFIGURATION ===
API_URL = "https://cms-bike-backend.qac24svc.dev/api/v1/misc/execute"
AUTH_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6IlNLMmNjOWZ4NnBMRXRXTGxGV3pQVVZrRGFiRDFKIiwidHlwIjoiSldUIn0.eyJhbXIiOlsib2F1dGgiXSwiZHJuIjoiRFMiLCJlbWFpbCI6Im1vaGQuYW1hYW4xQGNhcnMyNC5jb20iLCJleHAiOjE3NTAzMTA3MjcsImlhdCI6MTc0OTQ0NjcyNywiaXNzIjoiUDJjYzlmdlZwWHljY2QzcTdWZ0pCTTRnSWJhQiIsIm5hbWUiOiJNb2hkIEFtYWFuIiwicmV4cCI6IjIwMjUtMDctMDdUMDU6MjU6MjdaIiwic3ViIjoiVTJjdnVsSWN0TkpWa3NWMnZ0cGZFZ0NzQVRWZSIsInRlbmFudHMiOnsiMDMzN2YwMGQtZWMyYS00ZTFmLTg4NDEtMGI5ZWU2ZDA4NzI1Ijp7InBlcm1pc3Npb25zIjpbXSwicm9sZXMiOlsiUFVCTElTSEVSIl19LCIxNTMyYmZhMS1lM2Q4LTQyZGItODk4Ny01YWU1YmI0NzlkZjgiOnsicGVybWlzc2lvbnMiOltdLCJyb2xlcyI6WyJQVUJMSVNIRVIiXX0sIjVkY2IzODc5LTk4YTktNDJiNS04YjJkLTgwODU3OGZjZjVjMiI6eyJwZXJtaXNzaW9ucyI6W10sInJvbGVzIjpbIlBVQkxJU0hFUiJdfSwiNWZhMzI0YWItZjg5My00ZGQyLWI2YWUtZDg2MDZkNTdlNmI1Ijp7InBlcm1pc3Npb25zIjpbXSwicm9sZXMiOlsiUFVCTElTSEVSIl19LCI2YmE3YjgxMC05ZGFkLTExZDEtODBiNC0wMGMwNGZkNDMwYzgiOnsicGVybWlzc2lvbnMiOltdLCJyb2xlcyI6WyJQVUJMSVNIRVIiXX0sIjdjNjNkOTY2LTdkMDctNDUyYy04NjUzLWNiM2ViMDRjYTY3YyI6eyJwZXJtaXNzaW9ucyI6W10sInJvbGVzIjpbIlBVQkxJU0hFUiJdfSwiOGM2NGQ5NjYtN2QwNy00NTJjLTg2NTMtY2IzZWIwNGNhNDVhIjp7InBlcm1pc3Npb25zIjpbXSwicm9sZXMiOlsiUFVCTElTSEVSIl19LCI5YzU1YzZiOC1lZTFjLTRhN2YtYThiNC0zYzQxZDRlNGY2NTciOnsicGVybWlzc2lvbnMiOltdLCJyb2xlcyI6WyJQVUJMSVNIRVIiXX0sImJiZTNiZjU1LTMwYjUtNDU5Zi05M2IxLTNjYzU4NzFkYTkyNiI6eyJwZXJtaXNzaW9ucyI6W10sInJvbGVzIjpbIlBVQkxJU0hFUiJdfSwiZTJiOWM0YmMtOGRiMC00ZTJkLWIzYjQtZjk4YTkwZjNmNDliIjp7InBlcm1pc3Npb25zIjpbXSwicm9sZXMiOlsiUFVCTElTSEVSIl19LCJlNjRlYzdhNS02NzQzLTQ5ZWEtOTdmMy0zYzM3ZWQ5MDI1YmYiOnsicGVybWlzc2lvbnMiOltdLCJyb2xlcyI6WyJQVUJMSVNIRVIiXX0sImU5YTE3MGQ2LTM2NjktNDQxNi1iM2FkLTVhZTA2OGUwZjhhMiI6eyJwZXJtaXNzaW9ucyI6W10sInJvbGVzIjpbIlBVQkxJU0hFUiJdfSwiZWY3NTM0ZGUtOTMxZS00YzY4LTkzMmUtNjI2ZGExMDkyZjI5Ijp7InBlcm1pc3Npb25zIjpbXSwicm9sZXMiOlsiUFVCTElTSEVSIl19LCJlZjc1MzRkZS05MzFlLTRjNjgtOTMyZS02MjZkYTEwY2FyMjkiOnsicGVybWlzc2lvbnMiOltdLCJyb2xlcyI6WyJQVUJMSVNIRVIiXX0sImVmNzUzNGRlLTkzMWUtNGM2OC05MzJlLTYyNmRvbGQ5MmYzMyI6eyJwZXJtaXNzaW9ucyI6W10sInJvbGVzIjpbIlBVQkxJU0hFUiJdfSwiZjQ3YWMxMGItNThjYy00MzcyLWE1NjctMGUwMmIyYzNkNDc5Ijp7InBlcm1pc3Npb25zIjpbXSwicm9sZXMiOlsiUFVCTElTSEVSIl19fX0.Ly_h38RbjmVPbf_3q3rHxKXCNPZ3L7ioXqU6Cibkp_iknOT02Qwxfh14cCxDrO8wlgJ3dLKrxvBlQlZeqkWjJdNcQAeEGdz8sXnb-LFwaUvOudCMw123C9a7d9QOG9GS5wSiUoh8_Lal1gWmbjAfmkx_lZdGpw7FbTBvde9wp41Ug3j3hVc2vCKVkpuFt0d8BJe2g5i1UjtwTxmhfzMMvp_J4MjpRJAsYkQofJxImgSPIeC9SgIwo73wjeUZo51EHIm6BdE-vLpyEtUOtYtEJatqdNmrpz9kn-2KboTm5UGPViITsMGCDTGo66-wtZRSPg7ZKVbN2KxQQ2HExRSX1Q"  # 🔒 Replace with your actual bearer token
TARGET_SCHEMA = "cms_bike_backend_qa"
OUTPUT_DIR = "dev-db-data"
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {AUTH_TOKEN}"
}

# === SETUP OUTPUT FOLDER ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === GET TABLES FROM TARGET SCHEMA ===
def get_table_names():

    query = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{TARGET_SCHEMA}' AND table_type = 'BASE TABLE';
    """
    response = requests.post(API_URL, headers=HEADERS, json={"query": query})
    response.raise_for_status()
    result = response.json()

    if result.get("data", {}).get("success"):
        return [row["table_name"] for row in result["data"]["data"]]
    else:
        raise Exception("Failed to fetch table list")

# def dump_table_to_json(table_name):
#     query = f"SELECT * FROM {table_name};"
#     print(f"📦 Querying: {query}")
#     payload = {"query": query}
#
#     try:
#         response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
#         response.raise_for_status()
#         result = response.json()
#
#         if result.get("data", {}).get("success"):
#             rows = result["data"]["data"]
#             if not rows:
#                 print(f"⚠️  Empty table: {table_name}")
#                 return
#
#             out_file = os.path.join(OUTPUT_DIR, f"{table_name}_dump.json")
#             with open(out_file, "w", encoding="utf-8") as f:
#                 json.dump(rows, f, indent=2, ensure_ascii=False)
#             print(f"✅ Saved: {out_file}")
#         else:
#             print(f"❌ API Error: {result.get('error', {}).get('message', 'Unknown error')}")
#
#     except Exception as e:
#         print(f"❗ Failed to query {table_name}: {e}")

def fetch_actual_columns(table_name):
    """Fetch actual column names from information_schema for a table, with error handling."""
    query = f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{TARGET_SCHEMA}' AND TABLE_NAME = '{table_name}';
    """
    try:
        response = requests.post(API_URL, headers=HEADERS, json={"query": query}, timeout=20)
        response.raise_for_status()
        result = response.json()

        rows = result.get("data", {}).get("data", [])
        if not isinstance(rows, list) or not rows:
            raise ValueError(f"❌ Schema query returned empty or invalid format for table: {table_name}")

        columns = []
        for row in rows:
            col = row.get("COLUMN_NAME") or row.get("column_name")
            if col:
                columns.append(col)
            else:
                raise KeyError("⚠️ COLUMN_NAME not found in response row")
        return columns

    except Exception as e:
        print(f"❗ Failed to fetch columns for {table_name}: {e}")
        return []  # Return empty list so script can handle gracefully


def normalize_row_keys(rows, actual_columns):
    """Ensure row keys match actual table column names exactly."""
    normalized = []
    lower_col_map = {col.lower(): col for col in actual_columns}

    for row in rows:
        new_row = {}
        for key, value in row.items():
            actual_key = lower_col_map.get(key.lower())
            if actual_key:
                new_row[actual_key] = value
            else:
                # Optional: log or raise error for unexpected column
                print(f"⚠️ Unknown column '{key}' not in schema, skipping.")
        normalized.append(new_row)

    return normalized

# === DUMP EACH TABLE INTO JSON ===
def dump_table_to_json(table_name):
    query = f"SELECT * FROM {table_name};"
    print(f"📦 Querying: {query}")
    payload = {"query": query}

    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()

        if result.get("data", {}).get("success"):
            rows = result["data"]["data"]
            if not rows:
                print(f"⚠️  Empty table: {table_name}")
                return

            # 🧠 Normalize keys based on schema
            actual_columns = fetch_actual_columns(table_name)
            if not actual_columns:
                print(f"⚠️ Skipping {table_name} due to missing schema info.")
                return

            normalized_rows = normalize_row_keys(rows, actual_columns)

            # 💾 Save as JSON
            out_file = os.path.join(OUTPUT_DIR, f"{table_name}_dump.json")
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(normalized_rows, f, indent=2, ensure_ascii=False)
            print(f"✅ Saved: {out_file}")
        else:
            print(f"❌ API Error: {result.get('error', {}).get('message', 'Unknown error')}")

    except Exception as e:
        print(f"❗ Failed to query {table_name}: {e}")




# === MAIN EXECUTION ===
def main():
    try:
        tables = get_table_names()
        print(f"🔍 Found {len(tables)} tables in schema '{TARGET_SCHEMA}'")
        for table in tables:
            dump_table_to_json(table)
            time.sleep(0.3)  # 💤 avoid overwhelming the server
    except KeyboardInterrupt:
        print("🛑 Interrupted by user.")
    except Exception as e:
        print(f"❗ Fatal error: {e}")

def downlaod_db_data_from_dev(BEARER_TOKEN):
    if BEARER_TOKEN !=None:
        AUTH_TOKEN = BEARER_TOKEN
    main()