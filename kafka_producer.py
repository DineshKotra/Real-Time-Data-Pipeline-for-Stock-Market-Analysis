import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Stock symbols and their base prices
stocks = {
    'AAPL': 180.0,
    'MSFT': 350.0,
    'GOOGL': 130.0,
    'AMZN': 175.0,
    'META': 470.0
}

def generate_stock_data():
    """Generate simulated stock market data"""
    timestamp = datetime.now().isoformat()
    
    stock_data = []
    for symbol, base_price in stocks.items():
        # Simulate price movement
        price_change = random.uniform(-2.0, 2.0)
        current_price = base_price + price_change
        
        # Simulate trading volume
        volume = int(random.uniform(5000, 50000))
        
        # Add some randomness to bid/ask spread
        bid = current_price - random.uniform(0.01, 0.1)
        ask = current_price + random.uniform(0.01, 0.1)
        
        stock_data.append({
            'symbol': symbol,
            'timestamp': timestamp,
            'price': round(current_price, 2),
            'volume': volume,
            'bid': round(bid, 2),
            'ask': round(ask, 2)
        })
    
    return stock_data

def main():
    """Main function to produce stock data to Kafka"""
    print("Starting Kafka producer for stock market data...")
    
    try:
        while True:
            # Generate stock data
            stock_data = generate_stock_data()
            
            # Send each stock update to Kafka
            for data in stock_data:
                producer.send('stock-market-data', data)
                print(f"Sent: {data['symbol']} at ${data['price']}")
            
            # Flush to ensure all messages are sent
            producer.flush()
            
            # Wait before sending the next batch
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

