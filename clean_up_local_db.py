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
LOG_FILE = "restore_log.txt"

# ---- LOGGING ----
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(levelname)s:%(message)s")

# ---- MYSQL CONNECTION ----
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

# ---- FK DEPENDENCIES ----
def get_foreign_key_dependencies():
    query = """
    SELECT TABLE_NAME, REFERENCED_TABLE_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME IS NOT NULL
    """
    dependencies = []
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (CONFIG["database"],))
            for row in cursor.fetchall():
                dependencies.append((row["TABLE_NAME"], row["REFERENCED_TABLE_NAME"]))
    return dependencies

# ---- TOPOLOGICAL SORT ----
def topological_sort(tables, dependencies):
    graph = defaultdict(set)
    in_degree = defaultdict(int)

    for t in tables:
        in_degree[t] = 0

    for a, b in dependencies:
        graph[b].add(a)
        in_degree[a] += 1

    queue = deque([t for t in tables if in_degree[t] == 0])
    sorted_order = []

    while queue:
        node = queue.popleft()
        sorted_order.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(sorted_order) != len(tables):
        raise Exception("Cycle detected in foreign key graph")

    return sorted_order

# ---- DATA LOADING ----
def load_dump_data(table):
    file_path = os.path.join(DUMP_DIR, f"{table}.json")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"❌ Failed to load dump for {table}: {e}")
        return []

# ---- DATA INSERTION ----
def insert_data(table, data):
    if not data:
        logging.warning(f"⚠️  No data found for table: {table}")
        return

    columns = data[0].keys()
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SET FOREIGN_KEY_CHECKS = 1;")
                cursor.execute(f"TRUNCATE TABLE {table};")
                for i in range(0, len(data), 20):  # batch size = 20
                    batch = data[i:i + 20]
                    values = [tuple(row[col] for col in columns) for row in batch]
                    try:
                        cursor.executemany(insert_sql, values)
                    except pymysql.MySQLError as e:
                        logging.error(f"❌ Insert failed for table {table} (batch {i}-{i+len(batch)}): {e}")
    except pymysql.MySQLError as e:
        logging.error(f"❌ Unexpected failure for {table}: {e}")

# ---- MAIN EXECUTION ----
def restore_all_tables():
    dump_tables = [f[:-5] for f in os.listdir(DUMP_DIR) if f.endswith(".json")]
    print(f"🔍 Found {len(dump_tables)} tables in dump.")

    try:
        dependencies = get_foreign_key_dependencies()
        sorted_tables = topological_sort(dump_tables, dependencies)
    except Exception as e:
        logging.error(f"❌ Topological sort failed: {e}")
        sorted_tables = dump_tables  # fallback: unsorted

    for table in sorted_tables:
        print(f"📥 Restoring table: {table}")
        data = load_dump_data(table)
        insert_data(table, data)

    print("✅ Restoration process completed.")

# ---- ENTRY POINT ----
def clean_up_and_update_db_data():
    restore_all_tables()
