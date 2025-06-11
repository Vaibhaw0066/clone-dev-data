# 📦 clone-dev-data

A utility to populate your local MySQL database using structured JSON dump files, with support for foreign key validation and error-tolerant batch inserts.

---

## ⚙️ Prerequisites

- Python 3.8+
- MySQL running locally
- `requirements.txt` installed (see below)
- Update the `BEARER_TOKEN` used for the API call

---

## 🔧 Setup

1. **Clone the repository**  
   ```bash
   git clone https://github.com/Vaibhaw0066/clone-dev-data.git
   cd clone-dev-data
   ```

2. **Install dependencies**  
   ```bash
   pip install -r requirements.txt
   ```

3. **Update the `BEARER_TOKEN`**  
   Set the valid token inside the script or using an environment variable if required by your API setup.

---

## 🚀 Run the Script

From the project root directory:

  ```bash
    python3 run.py
  ```

This will:
- Read JSON files from the `dev-db-data/` folder
- Auto-create missing tables using local DB schema metadata
- Insert data with foreign key awareness
- Log errors if insert fails and continue with the next table

---

## 📝 Notes

- Ensure your MySQL config (host, port, user, password, database) matches your local setup.
- Dump files should be named `<table_name>.json` and placed under `dev-db-data/`.
