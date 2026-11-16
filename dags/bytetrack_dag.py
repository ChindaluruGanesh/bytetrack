from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import psycopg2

DB = dict(host='postgres', database='airflow', user='airflow', password='airflow')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

with DAG(
    dag_id='bytetrack_pipeline',
    default_args=default_args,
    description='ByteTrack Bronze -> Silver -> Gold pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['bytetrack', 'production'],
) as dag:

    # ============================================================
    # TASKS
    # ============================================================

    start = EmptyOperator(task_id='start')

    # 1. Load Bronze (CSVs/JSONs -> stg_*_raw tables)
    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command='python /opt/airflow/scripts/load_bronze.py',
        execution_timeout=timedelta(minutes=15),
    )

    # 2. Spark cleaner (Bronze -> Silver + DLQ)
    run_pyspark_cleaner = BashOperator(
        task_id='run_pyspark_cleaner',
        bash_command='python /opt/airflow/scripts/pyspark_cleaner.py',
        execution_timeout=timedelta(minutes=30),
    )

    # 3. dbt snapshot (SCD2 dims)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/dbt/bytetrack_dbt && dbt snapshot --profiles-dir .',
        execution_timeout=timedelta(minutes=10),
    )

    # 4. dbt run (facts + reports)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt/bytetrack_dbt && dbt run --profiles-dir .',
        execution_timeout=timedelta(minutes=10),
    )

    # 5. Reconcile — verify row counts are sane
    def reconcile(**context):
        conn = psycopg2.connect(**DB); cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM silver_orders_cleaned")
        silver = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM gold.fact_orders")
        fact = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM stg_rejected_level1")
        l1 = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM stg_rejected_level2")
        l2 = cur.fetchone()[0]
        
        cur.close(); conn.close()

        total_rejected = l1 + l2
        print(f"Silver orders: {silver}, Fact orders: {fact}, DLQ: L1={l1}, L2={l2}")
        
        # dbt snapshots add history rows to fact_orders, so fact > silver is normal.
        # We only fail if dbt somehow LOST data (fact < silver).
        if fact < silver:
            raise ValueError(f"Fact < Silver: {fact} < {silver} — dbt lost data!")
        if silver == 0:
            raise ValueError("Silver is empty — pipeline produced no clean rows")
            
        print(f"✓ Reconcile passed: {silver} clean rows, {fact} fact rows, {total_rejected} rejected")

    reconcile_task = PythonOperator(
        task_id='reconcile',
        python_callable=reconcile,
    )

    end = EmptyOperator(task_id='end')

    # ============================================================
    # DEPENDENCIES
    # ============================================================
    start >> load_bronze >> run_pyspark_cleaner >> dbt_snapshot >> dbt_run >> reconcile_task >> end