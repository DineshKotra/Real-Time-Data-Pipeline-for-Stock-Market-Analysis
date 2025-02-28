from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'stock_market_pipeline',
    default_args=default_args,
    description='A data pipeline for stock market analysis',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Create PostgreSQL tables if they don't exist
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS stock_analytics (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        symbol VARCHAR(10),
        avg_price DECIMAL(10, 2),
        max_price DECIMAL(10, 2),
        min_price DECIMAL(10, 2),
        avg_volume INTEGER,
        price_stddev DECIMAL(10, 4),
        PRIMARY KEY (window_start, window_end, symbol)
    );
    
    CREATE TABLE IF NOT EXISTS stock_anomalies (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        symbol VARCHAR(10),
        price_z_score DECIMAL(10, 4),
        anomaly_score DECIMAL(10, 2),
        anomaly_type VARCHAR(50),
        PRIMARY KEY (window_start, window_end, symbol)
    );
    
    CREATE TABLE IF NOT EXISTS daily_summary (
        date DATE,
        symbol VARCHAR(10),
        open_price DECIMAL(10, 2),
        close_price DECIMAL(10, 2),
        high_price DECIMAL(10, 2),
        low_price DECIMAL(10, 2),
        volume INTEGER,
        price_change_pct DECIMAL(10, 2),
        PRIMARY KEY (date, symbol)
    );
    """,
    dag=dag,
)

# Run Spark job for daily summary
spark_daily_summary = SparkSubmitOperator(
    task_id='spark_daily_summary',
    application='/opt/airflow/dags/scripts/daily_summary.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

# Run anomaly detection model training
train_anomaly_model = SparkSubmitOperator(
    task_id='train_anomaly_model',
    application='/opt/airflow/dags/scripts/train_anomaly_model.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

# Export data to AWS S3 for backup
export_to_s3 = PythonOperator(
    task_id='export_to_s3',
    python_callable=lambda: print("Exporting data to S3..."),
    dag=dag,
)

# Generate daily report
generate_report = PythonOperator(
    task_id='generate_report',
    python_callable=lambda: print("Generating daily report..."),
    dag=dag,
)

# Define task dependencies
create_tables >> spark_daily_summary >> train_anomaly_model >> export_to_s3 >> generate_report

