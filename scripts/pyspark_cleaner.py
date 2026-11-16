from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, to_json, struct, row_number,
    current_timestamp, length, year, trim, lower, initcap, to_timestamp, upper
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
import psycopg2
from datetime import datetime
import json

JDBC_URL = 'jdbc:postgresql://postgres:5432/airflow'
JDBC_PROPS = {'user': 'airflow', 'password': 'airflow', 'driver': 'org.postgresql.Driver', 'stringtype': 'unspecified'}
BRONZE_TABLES = ['stg_restaurants_raw', 'stg_customers_raw', 'stg_agents_raw', 'stg_orders_raw', 'stg_order_items_raw', 'stg_payments_raw', 'stg_complaints_raw']
SILVER_TABLES = ['silver_restaurants_cleaned', 'silver_customers_cleaned', 'silver_agents_cleaned', 'silver_orders_cleaned', 'silver_order_items_cleaned', 'silver_payments_cleaned', 'silver_complaints_cleaned']
DEDUP_CONFIG = {
    'stg_orders_raw':      {'p_k': 'order_id',      'ord_col': 'order_ts'},
    'stg_order_items_raw': {'p_k': 'order_item_id', 'ord_col': 'ingestion_ts'},
    'stg_restaurants_raw': {'p_k': 'restaurant_id', 'ord_col': 'ingestion_ts'},
    'stg_customers_raw':   {'p_k': 'customer_id',   'ord_col': 'ingestion_ts'},
    'stg_agents_raw':      {'p_k': 'agent_id',      'ord_col': 'ingestion_ts'},
    'stg_payments_raw':    {'p_k': 'payment_id',    'ord_col': 'payment_ts'},
    'stg_complaints_raw':  {'p_k': 'complaint_id',  'ord_col': 'complaint_ts'}
}
LEVEL_2_REASONS = [
    'Invalid status', 'Delivered but no delivery time', 'Delivery time before order time',
    'Cancelled order with non-zero amount', 'Discount exceeds total',
    'Invalid payment method', 'Invalid complaint category',
    'Orphan order_id — no matching order', 'Orphan customer_id — no matching customer',
    'Orphan restaurant_id — no matching restaurant', 'Orphan agent_id — no matching agent',
]

def get_conn(): return psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
def truncate_one(table):
    conn = get_conn(); cur = conn.cursor(); cur.execute(f"TRUNCATE TABLE {table}"); conn.commit(); cur.close(); conn.close()
def get_last_watermark(table_name):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT MAX(end_time) FROM audit_pipeline_runs WHERE table_name = %s AND status IN ('SUCCESS','PARTIAL_REJECTIONS')", (table_name,))
        r = cur.fetchone(); cur.close(); conn.close(); return r[0] if r else None
    except: return None
def is_first_ever_run():
    try:
        conn = get_conn(); cur = conn.cursor(); cur.execute("SELECT COUNT(*) FROM audit_pipeline_runs WHERE table_name = 'ALL'")
        c = cur.fetchone()[0]; cur.close(); conn.close(); return c == 0
    except: return True
def truncate_all_silver_tables():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("TRUNCATE TABLE silver_restaurants_cleaned, silver_customers_cleaned, silver_agents_cleaned, silver_orders_cleaned, silver_order_items_cleaned, silver_payments_cleaned, silver_complaints_cleaned RESTART IDENTITY CASCADE")
    conn.commit(); cur.close(); conn.close(); print("Truncated all Silver tables (first-ever bootstrap)")

def upsert_stage_to_silver(stage_table, silver_table, pk_col):
    orphans_total = 0
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (stage_table,))
    cols = [r[0] for r in cur.fetchall()]
    col_list = ', '.join(f'"{c}"' for c in cols)
    update_cols = [c for c in cols if c not in (pk_col, 'created_at', 'updated_at')]
    update_clause = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols) if update_cols else ''
    if 'updated_at' in cols: update_clause = (update_clause + ', ' if update_clause else '') + '"updated_at" = NOW()'
    
    fk_checks = {
        'silver_order_items_cleaned': [('order_id', 'silver_orders_cleaned')],
        'silver_payments_cleaned':    [('order_id', 'silver_orders_cleaned')],
        'silver_complaints_cleaned':  [('order_id', 'silver_orders_cleaned'), ('customer_id', 'silver_customers_cleaned')]
    }
    if silver_table in fk_checks:
        for fk_col, parent_table in fk_checks[silver_table]:
            sql_find = f"SELECT {col_list} FROM {stage_table} s WHERE NOT EXISTS (SELECT 1 FROM {parent_table} p WHERE p.{fk_col} = s.{fk_col})"
            cur.execute(sql_find); orphans = cur.fetchall()
            if orphans:
                for row in orphans:
                    payload = dict(zip(cols, row))
                    cur.execute("INSERT INTO stg_rejected_level2 (source_table, raw_data, error_reason, rejection_level, rejected_ts) VALUES (%s, %s, %s, %s, NOW())",
                        (silver_table.replace('silver_', 'stg_').replace('_cleaned', '_raw'), json.dumps(payload, default=str), f'Orphan {fk_col}', 2))
                cur.execute(f"DELETE FROM {stage_table} s WHERE NOT EXISTS (SELECT 1 FROM {parent_table} p WHERE p.{fk_col} = s.{fk_col})")
                print(f"  [DB] Caught {len(orphans)} orphans from {stage_table} -> DLQ L2")
                orphans_total += len(orphans)

    sql = f"INSERT INTO {silver_table} ({col_list}) SELECT {col_list} FROM {stage_table} ON CONFLICT (\"{pk_col}\") DO UPDATE SET {update_clause}"
    cur.execute(sql); rows_upserted = cur.rowcount
    cur.execute(f"TRUNCATE TABLE {stage_table}"); conn.commit(); cur.close(); conn.close()
    print(f"  Upserted {rows_upserted} rows into {silver_table}"); return rows_upserted, orphans_total

def insert_audit_summary(dag_run_id, metrics_list, start_time, end_time, status='SUCCESS', error_message=None):
    conn = get_conn(); cur = conn.cursor()
    total_read = sum(m['rows_read'] for m in metrics_list); total_good = sum(m['rows_good'] for m in metrics_list)
    total_bad = sum(m['rows_bad'] for m in metrics_list); total_dupes = sum(m['rows_dupes'] for m in metrics_list)
    if total_read != (total_good + total_bad + total_dupes): print(f"WARNING: Math off by {total_read - (total_good + total_bad + total_dupes)}")
    cur.execute("INSERT INTO audit_pipeline_runs (dag_run_id, table_name, rows_read, rows_inserted, rows_rejected, status, start_time, end_time, error_message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING run_id",
        (dag_run_id, 'ALL', total_read, total_good, total_bad, status, start_time, end_time, error_message))
    run_id = cur.fetchone()[0]; conn.commit(); cur.close(); conn.close(); print(f"Audit summary inserted. run_id: {run_id}"); return run_id

def insert_table_metrics(run_id, table_name, rows_read, rows_good, rows_bad, rows_dupes):
    conn = get_conn(); cur = conn.cursor()
    status = 'SUCCESS' if rows_bad == 0 else 'PARTIAL_REJECTIONS'
    cur.execute("INSERT INTO audit_pipeline_runs (dag_run_id, table_name, rows_read, rows_inserted, rows_rejected, status, start_time, end_time) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())",
        (str(run_id), table_name, rows_read, rows_good, rows_bad, status))
    conn.commit(); cur.close(); conn.close()

def validate_orders(df):
    return df.withColumn('error_reason', when(col('order_id').isNull(), 'Null order_id').when(col('customer_id').isNull(), 'Null customer_id').when(col('restaurant_id').isNull(), 'Null restaurant_id').when(col('order_ts').isNull(), 'Null order_ts').when(col('order_ts').isNotNull() & to_timestamp(col('order_ts')).isNull(), 'Unparseable order_ts').when(col('total_amount').isNotNull() & col('total_amount').cast('double').isNull(), 'Non-numeric amount').when((col('status') != 'cancelled') & (col('total_amount').cast('double') <= 0), 'Non-positive amount').when((col('status') == 'cancelled') & (col('total_amount').cast('double') != 0), 'Cancelled order with non-zero amount').when(to_timestamp(col('order_ts')) > current_timestamp(), 'Future order date').when(col('status').isNull(), 'Null status').when(~col('status').isin('delivered', 'cancelled', 'pending', 'refunded'), 'Invalid status').when((col('status') == 'delivered') & col('delivery_time_mins').isNull(), 'Delivered but no delivery time').when(col('delivery_ts').isNotNull() & (to_timestamp(col('delivery_ts')) <= to_timestamp(col('order_ts'))), 'Delivery time before order time').when((col('delivery_time_mins').cast('int') < 0) | (col('delivery_time_mins').cast('int') > 120), 'Delivery time out of range').when(year(to_timestamp(col('order_ts'))).isin([1990, 2050]), 'Extreme date value').when(col('discount_amount').cast('double') > col('total_amount').cast('double'), 'Discount exceeds total').otherwise(lit(None)))
def validate_customers(df):
    return df.withColumn('error_reason', when(col('customer_id').isNull(), "Null customer_id").when(col('signup_date').isNotNull() & to_timestamp(col('signup_date')).isNull(), "Unparseable signup_date").when(col('phone').isNotNull() & (length(col('phone')) != 10), "Invalid phone format").when(col('email').isNotNull() & (~col('email').contains('@') | ~col('email').contains('.')), "Invalid email format").when(to_timestamp(col('signup_date')) > current_timestamp(), "Future signup date").when((col('total_orders').cast('int') < 0) | (col('total_orders').cast('int') > 10000), "Total orders out of range").when(col('name').isNull() | (col('name') == ''), "Null or empty name").otherwise(lit(None)))
def validate_restaurants(df):
    return df.withColumn('error_reason', when(col('restaurant_id').isNull(), 'Null restaurant_id').when(col('join_date').isNotNull() & to_timestamp(col('join_date')).isNull(), 'Unparseable join_date').when(col('avg_rating').isNotNull() & ((col('avg_rating').cast('double') < 0) | (col('avg_rating').cast('double') > 5)), "Rating out of range").when((col('avg_prep_time_mins').cast('int') < 0) | (col('avg_prep_time_mins').cast('int') > 120), 'Prep time out of range').when((col('commission_pct').cast('double') < 0) | (col('commission_pct').cast('double') > 100), 'Commission out of range').when(col('is_active').isNull(), 'Null is_active').otherwise(lit(None)))
def validate_agents(df):
    return df.withColumn('error_reason', when(col('agent_id').isNull(), "Null agent_id").when(col('join_date').isNotNull() & to_timestamp(col('join_date')).isNull(), "Unparseable join_date").when(col('avg_rating').isNotNull() & ((col('avg_rating').cast('double') < 0) | (col('avg_rating').cast('double') > 5)), "Rating out of range").when((col('total_deliveries').cast('int') < 0) | (col('total_deliveries').cast('int') > 50000), "Total deliveries out of range").when(to_timestamp(col('join_date')) > current_timestamp(), "Future join date").when(col('name').isNull() | (col('name') == ''), "Null or empty name").otherwise(lit(None)))
def validate_payments(df):
    return df.withColumn('error_reason', when(col('payment_id').isNull(), "Null payment_id").when(col('payment_ts').isNull(), "Null payment_ts").when(col('payment_ts').isNotNull() & to_timestamp(col('payment_ts')).isNull(), "Unparseable payment_ts").when(col('amount').isNotNull() & (col('amount').cast('double').isNull() | (col('amount').cast('double') <= 0)), "Invalid or non-positive amount").when(col('payment_method').isNotNull() & ~col('payment_method').isin('UPI', 'Card', 'Cash', 'Wallet'), "Invalid payment method").when(col('status').isNull(), "Null status").when(~col('status').isin('success', 'failed', 'pending'), "Invalid status").when(col('order_id').isNull(), "Null order_id").when(to_timestamp(col('payment_ts')) > current_timestamp(), "Future payment date").otherwise(lit(None)))
def validate_complaints(df):
    return df.withColumn("error_reason", when(col('complaint_id').isNull(), "Null complaint_id").when(col('complaint_ts').isNull(), "Null complaint_ts").when(col('complaint_ts').isNotNull() & to_timestamp(col('complaint_ts')).isNull(), "Unparseable complaint_ts").when(col('order_id').isNull(), 'Null order_id').when(col('customer_id').isNull(), "Null customer_id").when(col('complaint_category').isNull(), "Null complaint category").when(~col('complaint_category').isin('Delivery Delay', 'Wrong Item', 'Quality Issue', 'Payment Problem'), 'Invalid complaint category').when(col('description').isNull() | (col('description') == ''), 'Null or empty description').when(to_timestamp(col('complaint_ts')) > current_timestamp(), 'Future complaint date').when(col('is_resolved').isNull(), 'Null is_resolved').otherwise(lit(None)))
def validate_order_items(df):
    return df.withColumn('error_reason', when(col('order_item_id').isNull(), "Null order_item_id").when(col('order_id').isNull(), "Null order_id").when(col('quantity').cast('int') < 0, "Negative quantity").when((col('unit_price').isNotNull()) & (col('unit_price').cast('double') < 0), "Negative unit price").when((col('total_price').isNotNull()) & (col('total_price').cast('double') < 0), "Negative total price").when(col('discount').cast('double') < 0, "Negative discount").otherwise(lit(None)))
def add_rejection_level(df): return df.withColumn('rejection_level', when(col('error_reason').isin(LEVEL_2_REASONS), lit(2)).otherwise(lit(1)))
def flag_orphans(df, valid_ids_df, fk_col, reason):
    valid_ids = valid_ids_df.select(col(fk_col).alias('_valid_fk')).distinct()
    df = df.join(valid_ids, df[fk_col] == valid_ids['_valid_fk'], 'left')
    return df.withColumn('error_reason', when(col('error_reason').isNotNull(), col('error_reason')).when(col(fk_col).isNotNull() & col('_valid_fk').isNull(), lit(reason)).otherwise(lit(None))).drop('_valid_fk')

CITY_MAP = {'Bnglr': 'Bangalore', 'Mum': 'Mumbai', 'Chenai': 'Chennai', 'Hydrabad': 'Hyderabad', 'Kolkta': 'Kolkata', 'Blore': 'Bangalore'}
CUISINE_MAP = {'Nrth Indian': 'North Indian', 'Sout Inian': 'South Indian', 'Chineese': 'Chinese', 'Italan': 'Italian', 'Birani': 'Biryani', 'Pia': 'Pizza'}
VEHICLE_MAP = {'Bikee': 'Bike', 'Scootr': 'Scooter', 'Carr': 'Car'}

def standardize_orders(df):
    for k, v in CITY_MAP.items(): df = df.withColumn('city', when(col('city') == k, v).otherwise(col('city')))
    for k, v in CUISINE_MAP.items(): df = df.withColumn('cuisine_type', when(col('cuisine_type') == k, v).otherwise(col('cuisine_type')))
    return df.withColumn('city', when(col('city').isNull(), 'Unknown').otherwise(initcap(trim(col('city'))))).withColumn('cuisine_type', when(col('cuisine_type').isNull(), 'Unknown').otherwise(initcap(trim(col('cuisine_type'))))).withColumn('zone', trim(col('zone'))).withColumn('status', lower(trim(col('status')))).withColumn('order_id', trim(col('order_id'))).withColumn('order_ts', to_timestamp(col('order_ts'))).withColumn('delivery_ts', to_timestamp(col('delivery_ts'))).withColumn('total_amount', col('total_amount').cast(DecimalType(12, 2))).withColumn('discount_amount', col('discount_amount').cast(DecimalType(12, 2))).withColumn('delivery_time_mins', col('delivery_time_mins').cast('int'))
def standardize_customers(df):
    for k, v in CITY_MAP.items(): df = df.withColumn('city', when(col('city') == k, v).otherwise(col('city')))
    return df.withColumn('city', when(col('city').isNull(), 'Unknown').otherwise(initcap(trim(col('city'))))).withColumn('name', initcap(trim(col('name')))).withColumn('loyalty_tier', when(col('loyalty_tier').isNull() | ~col('loyalty_tier').isin('Bronze','Silver','Gold'), 'Unknown').otherwise(initcap(trim(col('loyalty_tier'))))).withColumn('signup_date', to_timestamp(col('signup_date')).cast('date')).withColumn('total_orders', col('total_orders').cast('int'))
def standardize_restaurants(df):
    for k, v in CITY_MAP.items(): df = df.withColumn('city', when(col('city') == k, v).otherwise(col('city')))
    for k, v in CUISINE_MAP.items(): df = df.withColumn('cuisine_type', when(col('cuisine_type') == k, v).otherwise(col('cuisine_type')))
    return df.withColumn('city', when(col('city').isNull() | (col('city') == ''), 'Unknown').otherwise(initcap(trim(col('city'))))).withColumn('cuisine_type', when(col('cuisine_type').isNull() | (col('cuisine_type') == ''), 'Unknown').otherwise(initcap(trim(col('cuisine_type'))))).withColumn('name', initcap(trim(col('name')))).withColumn('avg_rating', col('avg_rating').cast(DecimalType(3, 2))).withColumn('avg_prep_time_mins', col('avg_prep_time_mins').cast('int')).withColumn('commission_pct', col('commission_pct').cast(DecimalType(5, 2))).withColumn('join_date', to_timestamp(col('join_date')).cast('date')).withColumn('is_active', col('is_active').cast('boolean'))
def standardize_agents(df):
    for k, v in CITY_MAP.items(): df = df.withColumn('city', when(col('city') == k, v).otherwise(col('city')))
    for k, v in VEHICLE_MAP.items(): df = df.withColumn('vehicle_type', when(col('vehicle_type') == k, v).otherwise(col('vehicle_type')))
    return df.withColumn('city', when(col('city').isNull() | (col('city') == ''), 'Unknown').otherwise(initcap(trim(col('city'))))).withColumn('vehicle_type', when(col('vehicle_type').isNull(), 'Unknown').otherwise(initcap(trim(col('vehicle_type'))))).withColumn('name', initcap(trim(col('name')))).withColumn('tier', when(col('tier').isNull() | ~col('tier').isin('Bronze','Silver','Gold'), 'Unknown').otherwise(initcap(trim(col('tier'))))).withColumn('avg_rating', col('avg_rating').cast(DecimalType(3, 2))).withColumn('total_deliveries', col('total_deliveries').cast('int')).withColumn('join_date', to_timestamp(col('join_date')).cast('date'))
def standardize_payments(df):
    return df.withColumn('payment_method', initcap(trim(col('payment_method')))).withColumn('gateway', initcap(trim(col('gateway')))).withColumn('status', lower(trim(col('status')))).withColumn('amount', col('amount').cast(DecimalType(12, 2))).withColumn('cashback', col('cashback').cast(DecimalType(12, 2))).withColumn('payment_ts', to_timestamp(col('payment_ts')))
def standardize_complaints(df):
    return df.withColumn('complaint_category', initcap(trim(col('complaint_category')))).withColumn('complaint_subcategory', initcap(trim(col('complaint_subcategory')))).withColumn('description', trim(col('description'))).withColumn('resolution_time_hrs', col('resolution_time_hrs').cast(DecimalType(5, 2))).withColumn('compensation_amount', col('compensation_amount').cast(DecimalType(12, 2))).withColumn('complaint_ts', to_timestamp(col('complaint_ts'))).withColumn('is_resolved', col('is_resolved').cast('boolean'))
def standardize_order_items(df):
    return df.withColumn('product_name', initcap(trim(col('product_name')))).withColumn('quantity', col('quantity').cast('int')).withColumn('unit_price', col('unit_price').cast(DecimalType(12, 2))).withColumn('total_price', col('total_price').cast(DecimalType(12, 2))).withColumn('discount', col('discount').cast(DecimalType(12, 2)))
def deduplicate(df, primary_key, order_col):
    window_spec = Window.partitionBy(primary_key).orderBy(col(order_col).desc())
    return df.withColumn('rank', row_number().over(window_spec)).filter(col('rank') == 1).drop('rank')

def print_distribution(metrics_list):
    print("\n" + "=" * 78)
    print(f"{'FINAL DISTRIBUTION':24}{'read':>10}{'dupes':>10}{'good':>10}{'bad':>10}")
    print("=" * 78)
    for m in metrics_list:
        flag = "" if m['rows_read'] == m['rows_good'] + m['rows_bad'] + m['rows_dupes'] else "  <-- MATH OFF"
        print(f"{m['table']:24}{m['rows_read']:>10,}{m['rows_dupes']:>10,}{m['rows_good']:>10,}{m['rows_bad']:>10,}{flag}")
    t = {k: sum(m[k] for m in metrics_list) for k in ('rows_read', 'rows_dupes', 'rows_good', 'rows_bad')}
    print("-" * 64)
    print(f"{'TOTAL':24}{t['rows_read']:>10,}{t['rows_dupes']:>10,}{t['rows_good']:>10,}{t['rows_bad']:>10,}")
    r = t['rows_read'] or 1
    print(f"{'PERCENT':24}{'':>10}{t['rows_dupes']/r:>10.2%}{t['rows_good']/r:>10.2%}{t['rows_bad']/r:>10.2%}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName('ByteTrack Cleaner').config('spark.sql.shuffle.partitions', 8).config('spark.driver.memory', '1536m').config('spark.driver.maxResultSize', '512m').config('spark.jars', '/opt/airflow/jars/postgresql-42.6.0.jar').getOrCreate()
    start_time = datetime.now(); RUN_BATCH_ID = start_time.strftime('%Y%m%d_%H%M%S'); DAG_RUN_ID = f"manual_{RUN_BATCH_ID}"
    SOURCE_SYSTEM_MAP = {'stg_orders_raw': 'OMS', 'stg_order_items_raw': 'OMS', 'stg_restaurants_raw': 'OMS', 'stg_customers_raw': 'OMS', 'stg_agents_raw': 'OMS', 'stg_payments_raw': 'PGW', 'stg_complaints_raw': 'CRM'}
    if is_first_ever_run(): truncate_all_silver_tables(); print("First-ever run — Silver bootstrapped.")
    valid_restaurant_ids_df = valid_customer_ids_df = valid_agent_ids_df = valid_order_ids_df = None
    metrics_list = []
    for table_name in BRONZE_TABLES:
        print(f"\nProcessing {table_name}...")
        watermark = get_last_watermark(table_name)
        read_source = f"(SELECT * FROM {table_name} WHERE ingestion_ts > '{watermark}') AS t" if watermark is not None else table_name
        df = spark.read.jdbc(url=JDBC_URL, table=read_source, properties=JDBC_PROPS)
        rows_read = df.count()
        config = DEDUP_CONFIG[table_name]; primary_key = config['p_k']; order_col = config['ord_col']
        for c in df.columns:
            if c.endswith('_id') and c != 'ingestion_ts': df = df.withColumn(c, upper(trim(col(c))))
        df_deduped = deduplicate(df, primary_key, order_col); rows_deduped = df_deduped.count(); rows_dupes = rows_read - rows_deduped; df = df_deduped
        if table_name == 'stg_orders_raw':
            df = validate_orders(df)
            if valid_customer_ids_df is not None: df = flag_orphans(df, valid_customer_ids_df, 'customer_id', 'Orphan customer_id — no matching customer')
            if valid_restaurant_ids_df is not None: df = flag_orphans(df, valid_restaurant_ids_df, 'restaurant_id', 'Orphan restaurant_id — no matching restaurant')
            if valid_agent_ids_df is not None: df = flag_orphans(df, valid_agent_ids_df, 'agent_id', 'Orphan agent_id — no matching agent')
        elif table_name == 'stg_customers_raw': df = validate_customers(df)
        elif table_name == 'stg_restaurants_raw': df = validate_restaurants(df)
        elif table_name == 'stg_agents_raw': df = validate_agents(df)
        elif table_name == 'stg_payments_raw':
            df = validate_payments(df)
            if valid_order_ids_df is not None: df = flag_orphans(df, valid_order_ids_df, 'order_id', 'Orphan order_id — no matching order')
        elif table_name == 'stg_complaints_raw':
            df = validate_complaints(df)
            if valid_order_ids_df is not None: df = flag_orphans(df, valid_order_ids_df, 'order_id', 'Orphan order_id — no matching order')
            if valid_customer_ids_df is not None: df = flag_orphans(df, valid_customer_ids_df, 'customer_id', 'Orphan customer_id — no matching customer')
        elif table_name == 'stg_order_items_raw':
            df = validate_order_items(df)
            if valid_order_ids_df is not None: df = flag_orphans(df, valid_order_ids_df, 'order_id', 'Orphan order_id — no matching order')
        df_good = df.filter(col('error_reason').isNull()); df_bad = df.filter(col('error_reason').isNotNull())
        rows_good = df_good.count(); rows_bad = df_bad.count()
        m = {'table': table_name, 'rows_read': rows_read, 'rows_good': rows_good, 'rows_bad': rows_bad, 'rows_dupes': rows_dupes}
        metrics_list.append(m)
        if rows_good > 0:
            std_map = {'stg_orders_raw': standardize_orders, 'stg_customers_raw': standardize_customers, 'stg_restaurants_raw': standardize_restaurants, 'stg_agents_raw': standardize_agents, 'stg_payments_raw': standardize_payments, 'stg_complaints_raw': standardize_complaints, 'stg_order_items_raw': standardize_order_items}
            df_good = std_map[table_name](df_good)
            for c in ['error_reason', 'rejection_level', 'rank']:
                if c in df_good.columns: df_good = df_good.drop(c)
            silver_table = SILVER_TABLES[BRONZE_TABLES.index(table_name)]; stage_table = f"{silver_table}_stage"
            truncate_one(stage_table)
            df_good_tagged = df_good.withColumn('batch_id', lit(RUN_BATCH_ID)).withColumn('source_system', lit(SOURCE_SYSTEM_MAP.get(table_name, 'OMS')))
            df_good_tagged.write.jdbc(url=JDBC_URL, table=stage_table, mode='append', properties=JDBC_PROPS)
            _, db_orphans = upsert_stage_to_silver(stage_table, silver_table, primary_key)
            if db_orphans:
                print(f"  [WARN] {db_orphans} rows passed the Spark checks but were orphans in the DB -> reclassified Good -> Bad")
                m['rows_good'] -= db_orphans; m['rows_bad'] += db_orphans
        if table_name == 'stg_restaurants_raw':
            valid_restaurant_ids_df = spark.read.jdbc(url=JDBC_URL, table='(SELECT restaurant_id FROM silver_restaurants_cleaned) AS t', properties=JDBC_PROPS).cache(); valid_restaurant_ids_df.count()
        elif table_name == 'stg_customers_raw':
            valid_customer_ids_df = spark.read.jdbc(url=JDBC_URL, table='(SELECT customer_id FROM silver_customers_cleaned) AS t', properties=JDBC_PROPS).cache(); valid_customer_ids_df.count()
        elif table_name == 'stg_agents_raw':
            valid_agent_ids_df = spark.read.jdbc(url=JDBC_URL, table='(SELECT agent_id FROM silver_agents_cleaned) AS t', properties=JDBC_PROPS).cache(); valid_agent_ids_df.count()
        elif table_name == 'stg_orders_raw':
            valid_order_ids_df = spark.read.jdbc(url=JDBC_URL, table='(SELECT order_id FROM silver_orders_cleaned) AS t', properties=JDBC_PROPS).cache(); valid_order_ids_df.count()
        if table_name == 'stg_orders_raw':
            # restaurants + agents are only parents of orders -> free them now.
            # customers + orders stay cached: items/payments/complaints still need them.
            for df_obj in [valid_restaurant_ids_df, valid_agent_ids_df]:
                if df_obj is not None: df_obj.unpersist()
            valid_restaurant_ids_df = valid_agent_ids_df = None
        elif table_name == 'stg_complaints_raw':   # last table -> release everything
            for df_obj in [valid_order_ids_df, valid_customer_ids_df]:
                if df_obj is not None: df_obj.unpersist()
            valid_order_ids_df = valid_customer_ids_df = None
        if rows_bad > 0:
            df_bad = add_rejection_level(df_bad); df_bad_l1 = df_bad.filter(col('rejection_level') == 1); df_bad_l2 = df_bad.filter(col('rejection_level') == 2)
            r1 = df_bad_l1.count(); r2 = df_bad_l2.count()
            if r1 > 0:
                df_bad_l1.select(lit(table_name).alias('source_table'), to_json(struct([c for c in df_bad_l1.columns if c not in ('error_reason', 'rejection_level')])).alias('raw_data'), col('error_reason'), lit(1).alias('rejection_level'), current_timestamp().alias('rejected_ts')).write.jdbc(url=JDBC_URL, table='stg_rejected_level1', mode='append', properties=JDBC_PROPS); print(f"  {r1} rows -> DLQ L1")
            if r2 > 0:
                df_bad_l2.select(lit(table_name).alias('source_table'), to_json(struct([c for c in df_bad_l2.columns if c not in ('error_reason', 'rejection_level')])).alias('raw_data'), col('error_reason'), lit(2).alias('rejection_level'), current_timestamp().alias('rejected_ts')).write.jdbc(url=JDBC_URL, table='stg_rejected_level2', mode='append', properties=JDBC_PROPS); print(f"  {r2} rows -> DLQ L2")
        print(f"  Read={rows_read} | Dupes={rows_dupes} | Good={m['rows_good']} | Bad={m['rows_bad']}")
    end_time = datetime.now(); run_id = insert_audit_summary(DAG_RUN_ID, metrics_list, start_time, end_time)
    if run_id:
        for m in metrics_list: insert_table_metrics(run_id, m['table'], m['rows_read'], m['rows_good'], m['rows_bad'], m['rows_dupes'])
    print_distribution(metrics_list)
    print(f"\nCLEANING COMPLETE — run_id={run_id}"); spark.stop()