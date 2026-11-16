import csv
import json
import os
import sys
from io import StringIO

import pandas as pd
import psycopg2

DATA_DIR = os.environ.get("BRONZE_DIR", "/opt/airflow/data/bronze")
DB_CONN = "host=postgres dbname=airflow user=airflow password=airflow"

FILES = {
    'stg_orders_raw': 'stg_orders_raw.csv',
    'stg_order_items_raw': 'stg_order_items_raw.csv',
    'stg_restaurants_raw': 'stg_restaurants_raw.csv',
    'stg_customers_raw': 'stg_customers_raw.csv',
    'stg_agents_raw': 'stg_agents_raw.csv',
    'stg_payments_raw': 'stg_payments_raw.json',
    'stg_complaints_raw': 'stg_complaints_raw.json',
}


def read_csv_tolerant(path):
    """Read a CSV as all-strings.  Lines with MORE fields than the header (schema drift) are
    TRUNCATED to the header width instead of being skipped, so no rows are lost silently.
    Short lines are padded with ''.  Returns (DataFrame, number_of_drifted_lines)."""
    rows, drifted = [], 0
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        n = len(header)
        for r in reader:
            if len(r) > n:
                r = r[:n]
                drifted += 1
            elif len(r) < n:
                r = r + [''] * (n - len(r))
            rows.append(r)
    return pd.DataFrame(rows, columns=header, dtype=str), drifted


def read_json_strings(path):
    """Read a JSON array of objects as all-strings (None -> '', True -> 'True')."""
    with open(path, encoding='utf-8') as f:
        records = json.load(f)
    clean = [{k: ('' if v is None else str(v)) for k, v in r.items()} for r in records]
    return pd.DataFrame(clean, dtype=str)


conn = psycopg2.connect(DB_CONN)
cur = conn.cursor()
failures = 0

for table, filename in FILES.items():
    path = os.path.join(DATA_DIR, filename)
    print(f"Loading {filename} -> {table}...")

    drifted = 0
    if filename.endswith('.csv'):
        df, drifted = read_csv_tolerant(path)
    else:
        df = read_json_strings(path)

    # Drop schema-drift garbage columns (if any survived)
    df = df[[c for c in df.columns if not str(c).startswith('SCHEMA_DRIFT_')]]
    df = df.fillna('')

    # Build column list from DataFrame (exclude ingestion_ts - Postgres will DEFAULT it)
    cols = [c for c in df.columns if c != 'ingestion_ts']
    df = df[cols]

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)

    try:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
        cur.copy_from(buffer, table, sep='\t', null='', columns=cols)
        conn.commit()
        extra = f" ({drifted:,} schema-drift lines truncated, not dropped)" if drifted else ""
        print(f"  [OK] Loaded {len(df):,} rows into {table}{extra}")
    except Exception as e:
        conn.rollback()
        failures += 1
        print(f"  [FAIL] {table}: {e}")

cur.close()
conn.close()
print("Done." if failures == 0 else f"Done with {failures} FAILED table(s).")
sys.exit(1 if failures else 0)