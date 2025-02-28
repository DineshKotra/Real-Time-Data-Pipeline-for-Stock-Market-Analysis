from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max, min, stddev, expr, 
    current_timestamp, lag, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType
)
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from .config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW_DATA,
    KAFKA_TOPIC_PROCESSED_DATA,
    KAFKA_TOPIC_ANOMALIES,
    MODEL_PATH,
    MODEL_VERSION,
    ANOMALY_THRESHOLD
)

def create_spark_session(app_name="StockMarketAnalysis"):
    """Create a Spark session for stream processing"""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate())

def define_schema():
    """Define the schema for stock market data"""
    return StructType([
        StructField("symbol", StringType()),
        StructField("timestamp", StringType()),
        StructField("price", DoubleType()),
        StructField("volume", LongType()),
        StructField("bid", DoubleType()),
        StructField("ask", DoubleType())
    ])

def read_kafka_stream(spark, schema):
    """Read data from Kafka stream"""
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC_RAW_DATA)
            .option("startingOffsets", "latest")
            .load()
            .selectExpr("CAST(value AS STRING) as json_data")
            .select(from_json("json_data", schema).alias("data"))
            .select("data.*")
            .withColumn("event_time", F.to_timestamp("timestamp")))

def process_stream(df):
    """Process the streaming data with various transformations"""
    
    # Calculate moving averages and other statistics
    analytics = (df.withWatermark("event_time", "1 minute")
                .groupBy(
                    window("event_time", "1 minute"),
                    "symbol")
                .agg(
                    avg("price").alias("avg_price"),
                    max("price").alias("max_price"),
                    min("price").alias("min_price"),
                    avg("volume").alias("avg_volume"),
                    stddev("price").alias("price_stddev")
                ))
    
    # Calculate price change percentage
    price_change = (df.withWatermark("event_time", "5 minutes")
                    .groupBy(
                        window("event_time", "5 minutes", "1 minute"),
                        "symbol")
                    .agg(
                        F.first("price").alias("start_price"),
                        F.last("price").alias("end_price")
                    )
                    .withColumn("price_change_pct", 
                                F.round((F.col("end_price") - F.col("start_price")) / 
                                      F.col("start_price") * 100, 2)))
    
    return analytics, price_change

def detect_anomalies(df):
    """Detect anomalies in the stock data"""
    # Calculate Z-score for price movements
    anomalies = (df.withColumn("price_z_score", 
                              (col("price") - col("avg_price")) / col("price_stddev"))
                 .filter(F.abs(col("price_z_score")) > ANOMALY_THRESHOLD)
                 .withColumn("anomaly_score", F.abs(col("price_z_score")) * 10)
                 .withColumn("anomaly_type", 
                            when(col("price_z_score") > 0, "Price spike")
                            .otherwise("Price drop")))
    
    return anomalies

def train_prediction_model(df):
    """Train a prediction model using historical data"""
    # Prepare features
    assembler = VectorAssembler(
        inputCols=["avg_price", "avg_volume", "price_stddev"],
        outputCol="features"
    )
    
    # Create training dataset
    training_data = assembler.transform(df)
    
    # Train model
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="price",
        numTrees=100
    )
    model = rf.fit(training_data)
    
    # Save model
    model.save(f"{MODEL_PATH}/rf_model_{MODEL_VERSION}")
    
    return model

def make_predictions(df, model):
    """Make price predictions using the trained model"""
    # Prepare features
    assembler = VectorAssembler(
        inputCols=["avg_price", "avg_volume", "price_stddev"],
        outputCol="features"
    )
    
    # Create prediction dataset
    prediction_data = assembler.transform(df)
    
    # Make predictions
    predictions = model.transform(prediction_data)
    
    return predictions.select(
        "symbol",
        "event_time",
        col("prediction").alias("predicted_price")
    )

def write_to_postgres(df, table_name, mode="append"):
    """Write processed data to PostgreSQL"""
    return (df.writeStream
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/stockmarket")
            .option("dbtable", table_name)
            .option("user", "postgres")
            .option("password", "postgres")
            .outputMode(mode)
            .start())

def main():
    """Main function to process stock market data with Spark"""
    spark = create_spark_session()
    schema = define_schema()
    
    # Read from Kafka
    stock_stream = read_kafka_stream(spark, schema)
    
    # Process the stream
    analytics, price_change = process_stream(stock_stream)
    
    # Detect anomalies
    anomalies = detect_anomalies(analytics)
    
    # Write to PostgreSQL
    analytics_query = write_to_postgres(
        analytics, 
        "stock_analytics"
    )
    
    anomaly_query = write_to_postgres(
        anomalies,
        "stock_anomalies"
    )
    
    # Train prediction model (periodically)
    if spark.catalog.tableExists("stock_analytics"):
        historical_data = spark.sql("""
            SELECT * FROM stock_analytics 
            WHERE window_start >= current_date - interval '30 days'
        """)
        model = train_prediction_model(historical_data)
        
        # Make predictions
        predictions = make_predictions(analytics, model)
        predictions_query = write_to_postgres(
            predictions,
            "stock_predictions"
        )
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()

