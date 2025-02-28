import logging
import asyncio
import aiohttp
import json
from datetime import datetime
from typing import Dict, List
import pandas as pd
from alpha_vantage.timeseries import TimeSeries
from kafka import KafkaProducer
from .config import (
    ALPHA_VANTAGE_API_KEY,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_DATA,
    STOCK_SYMBOLS,
    LOG_LEVEL,
    LOG_FORMAT
)

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class StockDataCollector:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
        self.session = None

    async def initialize(self):
        """Initialize aiohttp session"""
        self.session = aiohttp.ClientSession()

    async def close(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()

    async def fetch_stock_data(self, symbol: str) -> Dict:
        """Fetch real-time stock data for a given symbol"""
        try:
            # In a real implementation, you would use a proper stock market API
            # This is a simplified example using Alpha Vantage
            data, _ = self.ts.get_quote_endpoint(symbol)
            
            stock_data = {
                'symbol': symbol,
                'timestamp': datetime.now().isoformat(),
                'price': float(data['05. price']),
                'volume': int(data['06. volume']),
                'bid': float(data['08. bid price']),
                'ask': float(data['09. ask price'])
            }
            
            return stock_data
        
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def send_to_kafka(self, data: Dict):
        """Send data to Kafka topic"""
        try:
            self.producer.send(KAFKA_TOPIC_RAW_DATA, data)
            logger.debug(f"Sent data to Kafka: {data}")
        except Exception as e:
            logger.error(f"Error sending data to Kafka: {str(e)}")

    async def collect_data(self):
        """Main data collection loop"""
        try:
            await self.initialize()
            
            while True:
                for symbol in STOCK_SYMBOLS:
                    data = await self.fetch_stock_data(symbol)
                    if data:
                        self.send_to_kafka(data)
                
                # Wait for 1 minute before next fetch
                await asyncio.sleep(60)
        
        except KeyboardInterrupt:
            logger.info("Stopping data collection...")
        finally:
            await self.close()
            self.producer.close()

    def run(self):
        """Run the data collector"""
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.collect_data())
        finally:
            loop.close()

class HistoricalDataCollector:
    def __init__(self):
        self.ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')

    def fetch_historical_data(self, symbol: str, interval: str = 'daily') -> pd.DataFrame:
        """Fetch historical data for a given symbol"""
        try:
            if interval == 'daily':
                data, _ = self.ts.get_daily(symbol=symbol, outputsize='full')
            elif interval == 'weekly':
                data, _ = self.ts.get_weekly(symbol=symbol)
            elif interval == 'monthly':
                data, _ = self.ts.get_monthly(symbol=symbol)
            else:
                raise ValueError(f"Invalid interval: {interval}")

            # Rename columns to match our schema
            data.columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            data.index.name = 'date'
            
            # Add symbol column
            data['symbol'] = symbol
            
            # Calculate price change percentage
            data['price_change_pct'] = (
                (data['close_price'] - data['open_price']) / data['open_price'] * 100
            )
            
            return data

        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {str(e)}")
            return None

    def save_to_database(self, data: pd.DataFrame, table_name: str):
        """Save historical data to PostgreSQL"""
        from sqlalchemy import create_engine
        from .config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
        
        try:
            engine = create_engine(
                f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
            )
            
            # Save data to PostgreSQL
            data.to_sql(
                table_name,
                engine,
                if_exists='append',
                index=True,
                method='multi'
            )
            
            logger.info(f"Saved historical data to {table_name}")
        
        except Exception as e:
            logger.error(f"Error saving to database: {str(e)}")

def main():
    """Main function to run the data collector"""
    collector = StockDataCollector()
    collector.run()

if __name__ == "__main__":
    main()

