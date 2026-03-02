import pandas as pd
import logging

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

# # ============================================================================
# # DEFAULT ARGUMENTS
# # ============================================================================

default_args = {
    "owner": "charlie isra",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 8),
    "email": ["charlie.isra@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=2),
}

# # ============================================================================
# # DAG DEFINITIONS
# # ============================================================================
dag = DAG(
    dag_id="coffee_etl",
    default_args=default_args,
    description="Pipeline for dirty cafe data",
    schedule="@weekly",  # '@daily' for daily updates or '@hourly' for hourly updates
    catchup=False,
    tags=["coffee", "etl", "csv"],
    max_active_runs=1,
)

# # ============================================================================
# # VARIABLES
# # ============================================================================
ITEM_PRICES = {
    "coffee": 2,
    "tea": 1.5,
    "sandwich": 4,
    "salad": 5,
    "cake": 3,
    "cookie": 1,
    "smoothie": 4,
    "juice": 3,
}

EXPECTED_COLUMNS = [
    "transaction_id",
    "item",
    "quantity",
    "price_per_unit",
    "total_spent",
    "payment_method",
    "location",
    "transaction_date",
]


# ============================================================================
# PYTHON FUNCTIONS FOR CUSTOM TASKS
# ============================================================================
def check_dbconnection(**context):
    hook = MySqlHook(mysql_conn_id="mysql_coffee_db")
    hook.run("SELECT 1;")
    print("MySql connection was made. You may proceed.")


# # Ingesting Layer
# Kaggle documentation: missing or invalid values shown as 'ERROR' or 'UNKNOWN'
def csv_ingest():
    filepath = "/Users/charlie/Documents/ML Projects/Data, Native/dirty_cafe_sales_test-duplicates.csv"
    df = pd.read_csv(filepath, na_values=["ERROR", "UNKNOWN"])
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )  # Normalize column names

    if list(df.columns) != EXPECTED_COLUMNS:
        logging.warning(
            f"Schema mismatch during ingestion. Expected {EXPECTED_COLUMNS}, got {list(df.columns)}"
        )

    batch_id = "dirty_cafe_sales.csv"

    na_values_raw = df.isna().sum()
    logging.info(f"{batch_id} read at {datetime.now()}")
    logging.info(f"Missing values per column at csv_ingest: {na_values_raw.to_dict()}")
    return df, batch_id


## Validation Layer, String
def validate_string(df):
    # Make sure all string columns are lower case and replace NaN values with "unknown"
    str_columns = ["item", "payment_method", "location"]
    str_columns = [c for c in str_columns if c in df.columns]

    df[str_columns] = df[str_columns].astype("string")
    df[str_columns] = df[str_columns].apply(lambda x: x.str.lower())
    # df[str_columns] = df[str_columns].fillna("unknown")
    string_df = df
    return string_df


# Validation Layer, Duplicated transaction IDs
def validate_ids(df, batch_id, engine):
    """Check for duplicated transaction IDs"""
    error_df = df[df.duplicated(subset=["transaction_id"], keep=False)].copy()
    valid_df = df[~df.index.isin(error_df.index)].copy()
    if not error_df.empty:
        error_df["batch_id"] = batch_id
        error_df["run_time"] = datetime.now()
        logging.error(
            f"Saved {len(error_df)} errors to duplicate_transaction_ids (table) for batch id: {batch_id}"
        )
        error_df.to_sql(
            "duplicate_transaction_ids", engine, if_exists="replace", index=False
        )
    return valid_df, error_df


## Transformation Layer, Fill Missing Values
def transform_fill(df):
    missing_prices = df["price_per_unit"].isna().sum()
    missing_qty = df["quantity"].isna().sum()
    missing_total = df["total_spent"].isna().sum()
    logging.info(
        f" Number of values missing before fill - \
            price_per_unit: {missing_prices}, \
            quantity: {missing_qty}, \
            total_spent: {missing_total} "
    )
    num_columns = ["quantity", "price_per_unit", "total_spent"]
    df[num_columns] = df[num_columns].apply(pd.to_numeric, errors="coerce")

    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], format="%Y-%m-%d", errors="coerce"
    )

    # Fill in 'price_per_unit' values using prices in the documentation first.
    mapped_prices = df["item"].map(ITEM_PRICES)
    df["price_per_unit"] = df["price_per_unit"].fillna(mapped_prices)

    # Compute missing values (if at least 2 of the values are known)
    total_amount = df["quantity"] * df["price_per_unit"]
    unit_price = df["total_spent"] / df["quantity"]
    quantity = df["total_spent"] / df["price_per_unit"]

    # Fill missing values
    df["quantity"] = df["quantity"].fillna(quantity)
    df["price_per_unit"] = df["price_per_unit"].fillna(unit_price)
    df["total_spent"] = df["total_spent"].fillna(total_amount)
    fill_df = df

    missing_prices = df["price_per_unit"].isna().sum()
    missing_qty = df["quantity"].isna().sum()
    missing_total = df["total_spent"].isna().sum()
    logging.info(
        f" Number of values missing after fill - \
            price_per_unit: {missing_prices}, \
            quantity: {missing_qty}, \
            total_spent: {missing_total} "
    )
    logging.info(f"dtypes after transform_fill {fill_df.dtypes.to_dict()}")
    return fill_df


# Transformation, reject data
def transform_drop(df):
    total_rows = len(df)
    na_rows = df.isna().any(axis=1).sum()

    logging.info(
        f"Rows with at least one null before drop: {na_rows} out of {total_rows}"
    )

    # Deleting rows that have nulls if they are less than 2% of data.
    if total_rows > 0 and na_rows / len(df) < 0.02:
        clean_df = df.dropna()
        logging.info(f"{na_rows} rows with null values deleted (<2% threshold). ")
    else:
        clean_df = df
        logging.warning(f"{na_rows} rows with null values kept (>=2% threshold).")

    return clean_df


# Load processed data
def load_clean_data(clean_df, engine, **context):
    load_date = context["logical_date"].date()
    clean_df["load_date"] = load_date  # Add metadata column
    clean_df.to_sql("coffee_clean_stg", engine, if_exists="replace", index=False)
    print(f"Loaded {len(clean_df)} rows successfully.")


def run_ETL(**context):
    hook = MySqlHook(mysql_conn_id="mysql_coffee_db")
    engine = hook.get_sqlalchemy_engine()

    df, batch_id = csv_ingest()
    string_df = validate_string(df)
    valid_df, error_df = validate_ids(string_df, batch_id, engine)
    fill_df = transform_fill(valid_df)
    clean_df = transform_drop(fill_df)

    load_clean_data(clean_df, engine, **context)

    # ETL Report
    logging.info(
        f"ETL Summary: {batch_id} (Batch id) had {len(clean_df)} rows loaded and"
        f"{len(error_df)} duplicate rows rejected."
    )


# # ============================================================================
# # TASK DEFINITIONS
# # ============================================================================

t1_check_dbconnection = PythonOperator(
    task_id="check_dbconnection",
    python_callable=check_dbconnection,
    provide_context=True,
    dag=dag,
)

t2_load_to_staging = PythonOperator(
    task_id="run_ETL",
    python_callable=run_ETL,
    provide_context=True,
    dag=dag,
)

t3_load_to_production = MySqlOperator(  # Wrapping SQL in an Airflow operator
    task_id="load_staging_to_prod",
    mysql_conn_id="mysql_coffee_db",
    sql="""
        DELETE FROM coffee_production -- Idempotent: Deletes same day data 
        WHERE load_date = '{{ ds }}';

        INSERT INTO coffee_production (
            transaction_id,
            item,
            quantity,
            price_per_unit,
            total_spent,
            payment_method,
            location,
            transaction_date,
            load_date
        )
        SELECT
            transaction_id,
            item,
            quantity,
            price_per_unit,
            total_spent,
            payment_method,
            location,
            transaction_date,
            load_date
        FROM coffee_clean_stg
        WHERE load_date = '{{ ds }}';    
    """,
    dag=dag,
)

t4_update_dim = MySqlOperator(
    task_id="update_dim",
    mysql_conn_id="mysql_coffee_db",
    sql="""
        INSERT INTO item_dimension (item_code, item_category, base_price_per_unit)
        SELECT DISTINCT
            s.item AS item_code,
            CASE
                WHEN s.item IN ('coffee', 'tea', 'smoothie', 'juice') THEN 'drink'
                ELSE 'food'
            END AS item_category,
            s.price_per_unit AS base_price_per_unit
        FROM coffee_clean_stg s
        LEFT JOIN item_dimension d
            ON d.item_code = s.item
        WHERE s.load_date = '{{ ds }}'
          AND d.item_code IS NULL
          AND s.item IS NOT NULL
          AND s.item <> 'nan'
          AND s.item <> '';
        """,
    dag=dag,
)

t5_build_fact_table = MySqlOperator(
    task_id="build_fact_table",
    mysql_conn_id="mysql_coffee_db",
    sql="""
        INSERT INTO coffee_sales_fact (
            item_id,
            quantity,
            total_spent,
            transaction_date,
            transaction_id_source
        )	
        SELECT
            d.item_id,
            p.quantity,
            p.total_spent,
            p.transaction_date,
            p.transaction_id
        FROM coffee_production p 
        JOIN item_dimension d
        ON d.item_code = p.item
        """,
    dag=dag,
)
# ============================================================================
# TASK DEPENDENCIES
# ============================================================================
# Task orchestration: First, check database connection. Second, load raw data
(
    t1_check_dbconnection
    >> t2_load_to_staging
    >> t3_load_to_production
    >> t4_update_dim
    >> t5_build_fact_table
)

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================
dag.doc_md = """
# Coffee ETL Pipeline

## Overview
This DAG orchestrates the daily ETL process for the `coffee_db` logistics database.

## Tasks
1. **check_db_connectivity**: Validates MySQL connection
2. **load_to_staging**: Extracts transaction_data from csv

## Schedule
- **Frequency**: Weekly
- **Time**: 2:00 AM UTC
- **Timezone**: UTC

## MySQL Connection
- **Connection ID**: mysql_coffee_db
- **Schema**: coffee_db
- **Host**: [Set in Airflow Connections]
- **Port**: 3306

## Setup Requirements
1. Create MySQL connection in Airflow UI with connection ID `mysql_coffee_db`
2. Ensure MySQL user has SELECT permissions on `coffee_db` schema
3. Set AIRFLOW_HOME=/Users/name/airflow in your environment

## Monitoring
- Failed tasks will trigger email alerts
- Check Airflow for task logs 
- Review DAG runs history for performance metrics

## Notes
- DAG respects PST timezone for scheduling
- Maximum 1 concurrent DAG run to prevent database locks
"""
