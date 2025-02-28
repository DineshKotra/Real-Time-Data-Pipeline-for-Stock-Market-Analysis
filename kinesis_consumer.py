import json
import boto3
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# AWS Kinesis configuration
REGION = 'us-east-1'
STREAM_NAME = 'stock-market-stream'

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DATABASE = 'stockmarket'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'

def get_kinesis_client():
    """Create and return a Kinesis client"""
    return boto3.client('kinesis', region_name=REGION)

def get_postgres_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

def process_records(records, conn):
    """Process records from Kinesis and store in PostgreSQL"""
    if not records:
        return
    
    cursor = conn.cursor()
    stock_data = []
    
    for record in records:
        try:
            # Decode and parse the record data
            data = json.loads(record['Data'].decode('utf-8'))
            
            # Extract fields
            symbol = data['symbol']
            timestamp = datetime.fromisoformat(data['timestamp'])
            price = data['price']
            volume = data['volume']
            bid = data['bid']
            ask = data['ask']
            
            # Add to batch
            stock_data.append((symbol, timestamp, price, volume, bid, ask))
            
        except Exception as e:
            print(f"Error processing record: {e}")
    
    # Insert batch into PostgreSQL
    if stock_data:
        try:
            execute_values(
                cursor,
                """
                INSERT INTO stock_raw_data 
                (symbol, timestamp, price, volume, bid, ask)
                VALUES %s
                ON CONFLICT (symbol, timestamp) DO UPDATE 
                SET price = EXCLUDED.price,
                    volume = EXCLUDED.volume,
                    bid = EXCLUDED.bid,
                    ask = EXCLUDED.ask
                """,
                stock_data
            )
            conn.commit()
            print(f"Inserted {len(stock_data)} records into PostgreSQL")
        except Exception as e:
            conn.rollback()
            print(f"Error inserting into PostgreSQL: {e}")
    
    cursor.close()

def main():
    """Main function to consume from Kinesis and store in PostgreSQL"""
    kinesis_client = get_kinesis_client()
    
    # Get shard iterator
    response = kinesis_client.describe_stream(StreamName=STREAM_NAME)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    
    shard_iterator_response = kinesis_client.get_shard_iterator(
        StreamName=STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType='LATEST'
    )
    shard_iterator = shard_iterator_response['ShardIterator']
    
    # Connect to PostgreSQL
    conn = get_postgres_connection()
    
    print(f"Starting Kinesis consumer for stream: {STREAM_NAME}")
    
    try:
        # Continuously read from the Kinesis stream
        while True:
            response = kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=100
            )
            
            # Process the records
            process_records(response['Records'], conn)
            
            # Get the next shard iterator
            shard_iterator = response['NextShardIterator']
            
            # Wait before the next poll
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

