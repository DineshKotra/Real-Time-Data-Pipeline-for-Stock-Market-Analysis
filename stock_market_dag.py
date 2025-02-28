from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import os

# Add pipeline directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_collector import StockDataCollector, HistoricalDataCollector
from config import STOCK_SYMBOLS

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_market_pipeline',
    default_args=default_args,
    description='A data pipeline for stock market analysis',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Task to check database connectivity
check_db = PostgresOperator(
    task_id='check_database',
    postgres_conn_id='postgres_default',
    sql="SELECT 1;",
    dag=dag,
)

# Task to collect historical data
def collect_historical_data():
    collector = HistoricalDataCollector()
    for symbol in STOCK_SYMBOLS:
        # Collect daily data
        data = collector.fetch_historical_data(symbol, 'daily')
        if data is not None:
            collector.save_to_database(data, 'daily_summary')
        
        # Collect weekly data
        data = collector.fetch_historical_data(symbol, 'weekly')
        if data is not None:
            collector.save_to_database(data, 'weekly_summary')

collect_historical = PythonOperator(
    task_id='collect_historical_data',
    python_callable=collect_historical_data,
    dag=dag,
)

# Task to start real-time data collection
def start_realtime_collection():
    collector = StockDataCollector()
    collector.run()

realtime_collection = PythonOperator(
    task_id='realtime_collection',
    python_callable=start_realtime_collection,
    dag=dag,
)

# Task to process data with Spark
process_data = SparkSubmitOperator(
    task_id='process_data',
    application='/opt/airflow/dags/spark_processor.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

# Task to train ML models
train_models = SparkSubmitOperator(
    task_id='train_models',
    application='/opt/airflow/dags/train_models.py',
    conn_id='spark_default',
    verbose=True,
    dag=dag,
)

# Task to generate daily reports
generate_reports = PostgresOperator(
    task_id='generate_reports',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO daily_summary (
        symbol, date, open_price, close_price, 
        high_price, low_price, volume, price_change_pct
    )
    SELECT 
        symbol,
        DATE(window_start) as date,
        FIRST_VALUE(price) OVER (PARTITION BY symbol, DATE(window_start) ORDER BY window_start) as open_price,
        LAST_VALUE(price) OVER (PARTITION BY symbol, DATE(window_start) ORDER BY window_start) as close_price,
        MAX(price) as high_price,
        MIN(price) as low_price,
        SUM(volume) as volume,
        ((LAST_VALUE(price) OVER (PARTITION BY symbol, DATE(window_start) ORDER BY window_start) - 
          FIRST_VALUE(price) OVER (PARTITION BY symbol, DATE(window_start) ORDER BY window_start)) / 
         FIRST_VALUE(price) OVER (PARTITION BY symbol, DATE(window_start) ORDER BY window_start)) * 100 as price_change_pct
    FROM stock_raw_data
    WHERE DATE(window_start) = CURRENT_DATE - INTERVAL '1 day'
    GROUP BY symbol, DATE(window_start)
    ON CONFLICT (symbol, date) DO UPDATE
    SET 
        open_price = EXCLUDED.open_price,
        close_price = EXCLUDED.close_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        volume = EXCLUDED.volume,
        price_change_pct = EXCLUDED.price_change_pct;
    """,
    dag=dag,
)

# Task to cleanup old data
cleanup_old_data = PostgresOperator(
    task_id='cleanup_old_data',
    postgres_conn_id='postgres_default',
    sql="""
    -- Delete raw data older than 7 days
    DELETE FROM stock_raw_data 
    WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days';
    
    -- Delete analytics older than 30 days
    DELETE FROM stock_analytics 
    WHERE window_start < CURRENT_TIMESTAMP - INTERVAL '30 days';
    
    -- Delete old anomalies
    DELETE FROM stock_anomalies 
    WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '30 days';
    
    -- Delete old predictions
    DELETE FROM stock_predictions 
    WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days';
    """,
    dag=dag,
)

# Define task dependencies
check_db >> collect_historical >> realtime_collection >> process_data >> train_models >> generate_reports >> cleanup_old_data

