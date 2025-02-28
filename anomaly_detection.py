import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# PostgreSQL configuration
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DATABASE = 'stockmarket'
PG_USER = 'postgres'
PG_PASSWORD = 'postgres'

def get_postgres_connection():
    """Create and return a PostgreSQL connection"""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

def fetch_training_data(conn, symbol, days=30):
    """Fetch historical data for training the anomaly detection model"""
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT 
        date,
        symbol,
        open_price,
        close_price,
        high_price,
        low_price,
        volume,
        price_change_pct
    FROM 
        daily_summary
    WHERE 
        symbol = %s
        AND date >= CURRENT_DATE - INTERVAL '%s days'
    ORDER BY 
        date
    """
    
    cursor.execute(query, (symbol, days))
    data = cursor.fetchall()
    cursor.close()
    
    return pd.DataFrame(data)

def prepare_features(df):
    """Prepare features for anomaly detection"""
    # Calculate additional features
    df['price_range'] = df['high_price'] - df['low_price']
    df['price_range_pct'] = df['price_range'] / df['open_price'] * 100
    df['volume_change'] = df['volume'].pct_change()
    
    # Fill NaN values
    df = df.fillna(0)
    
    # Select features for anomaly detection
    features = df[['price_change_pct', 'price_range_pct', 'volume_change']]
    
    # Scale features
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    
    return scaled_features, scaler

def train_model(features):
    """Train an Isolation Forest model for anomaly detection"""
    # Initialize and train the model
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,  # Assume 5% of data points are anomalies
        random_state=42
    )
    model.fit(features)
    
    return model

def save_model(model, scaler, symbol):
    """Save the trained model and scaler"""
    joblib.dump(model, f'models/anomaly_detection_{symbol}.pkl')
    joblib.dump(scaler, f'models/scaler_{symbol}.pkl')
    logger.info(f"Model for {symbol} saved successfully")

def detect_anomalies(conn, symbol):
    """Detect anomalies in recent stock data"""
    # Load the model and scaler
    try:
        model = joblib.load(f'models/anomaly_detection_{symbol}.pkl')
        scaler = joblib.load(f'models/scaler_{symbol}.pkl')
    except FileNotFoundError:
        logger.error(f"Model for {symbol} not found. Train the model first.")
        return
    
    # Fetch recent data
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
    SELECT 
        window_start,
        symbol,
        avg_price,
        max_price,
        min_price,
        avg_volume,
        price_stddev
    FROM 
        stock_analytics
    WHERE 
        symbol = %s
        AND window_start >= NOW() - INTERVAL '1 hour'
    ORDER BY 
        window_start
    """
    
    cursor.execute(query, (symbol,))
    recent_data = cursor.fetchall()
    cursor.close()
    
    if not recent_data:
        logger.info(f"No recent data found for {symbol}")
        return
    
    # Prepare data
    df = pd.DataFrame(recent_data)
    df['price_range'] = df['max_price'] - df['min_price']
    df['price_range_pct'] = df['price_range'] / df['avg_price'] * 100
    df['volume_change'] = df['avg_volume'].pct_change().fillna(0)
    
    features = df[['price_range_pct', 'volume_change']]
    features['price_stddev_norm'] = df['price_stddev'] / df['avg_price'] * 100
    
    # Scale features
    scaled_features = scaler.transform(features)
    
    # Predict anomalies
    anomaly_scores = model.decision_function(scaled_features)
    anomaly_predictions = model.predict(scaled_features)
    
    # Convert predictions (-1 for anomalies, 1 for normal)
    df['is_anomaly'] = np.where(anomaly_predictions == -1, True, False)
    df['anomaly_score'] = 100 * (0.5 - (anomaly_scores + 0.5))  # Convert to 0-100 scale
    
    # Log anomalies
    anomalies = df[df['is_anomaly']]
    if not anomalies.empty:
        logger.info(f"Detected {len(anomalies)} anomalies for {symbol}")
        
        # Insert anomalies into database
        insert_anomalies(conn, anomalies)
    else:
        logger.info(f"No anomalies detected for {symbol}")

def insert_anomalies(conn, anomalies_df):
    """Insert detected anomalies into the database"""
    cursor = conn.cursor()
    
    for _, row in anomalies_df.iterrows():
        try:
            cursor.execute(
                """
                INSERT INTO stock_anomalies 
                (window_start, symbol, anomaly_score, anomaly_type)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (window_start, symbol) DO UPDATE 
                SET anomaly_score = EXCLUDED.anomaly_score,
                    anomaly_type = EXCLUDED.anomaly_type
                """,
                (
                    row['window_start'],
                    row['symbol'],
                    row['anomaly_score'],
                    'Price volatility' if row['price_range_pct'] > 1.0 else 'Volume spike'
                )
            )
        except Exception as e:
            logger.error(f"Error inserting anomaly: {e}")
    
    conn.commit()
    cursor.close()

def main():
    """Main function to train models and detect anomalies"""
    conn = get_postgres_connection()
    
    # List of stock symbols to monitor
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    
    try:
        # Train models for each symbol
        for symbol in symbols:
            logger.info(f"Training model for {symbol}")
            
            # Fetch training data
            df = fetch_training_data(conn, symbol)
            
            if df.empty:
                logger.warning(f"No training data available for {symbol}")
                continue
            
            # Prepare features and train model
            features, scaler = prepare_features(df)
            model = train_model(features)
            
            # Save the model
            save_model(model, scaler, symbol)
            
            # Detect anomalies
            detect_anomalies(conn, symbol)
    
    except Exception as e:
        logger.error(f"Error in anomaly detection pipeline: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

