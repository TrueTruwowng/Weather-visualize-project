import sys
import os
from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

#Load Environment Variables
load_dotenv()

SYSTEM_TZ = os.getenv("SYSTEM_TZ", "Asia/Ho_Chi_Minh")
os.environ['TZ'] = SYSTEM_TZ

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_data")

PG_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
CHECKPOINT_DIR = os.path.abspath(os.getenv("SPARK_CHECKPOINT_PATH", "./tmp/checkpoints/weather_to_db"))

logger.info("Starting Weather Consumer...")
logger.info(f"Kafka Servers: {KAFKA_SERVERS}")
logger.add(
    "logs/weather_process_consumer.log",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    encoding="utf-8",
    level="INFO"
)

try:
    spark = SparkSession.builder \
        .appName("WeatherConsumer") \
        .config("spark.sql.session.timeZone", SYSTEM_TZ) \
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone={SYSTEM_TZ}") \
        .config("spark.executor.extraJavaOptions", f"-Duser.timezone={SYSTEM_TZ}") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark Session created successfully.")
except Exception as e:    
    logger.critical(f"Failed to create Spark Session: {e}")
    sys.exit(1)

schema = StructType([
    StructField("city", StringType(), True),
    StructField("province", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)
])

def save_to_postgres(batch_df, batch_id):
    # Cache the batch DataFrame to optimize multiple actions
    batch_df.cache() 
    
    if batch_df.count() > 0:
        try:
            batch_df.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "city", "province", "avg_temp", "avg_wind_speed"
            ).write.jdbc(
                url=PG_URL,
                table="weather_stats",
                mode="append",
                properties={
                    "user": os.getenv("POSTGRES_USER"),
                    "password": os.getenv("POSTGRES_PASSWORD"),
                    "driver": "org.postgresql.Driver",
                    "stringtype": "unspecified",
                    # set timezone for the session to ensure correct timestamp handling
                    "sessionInitStatement": f"SET TIME ZONE '{SYSTEM_TZ}'"
                }
            )
            logger.info(f"Batch {batch_id} saved to Postgres.")
        except Exception as e:
            logger.error(f"Postgres Error: {e}")
    
    batch_df.unpersist() # Release cache after processing

### kafka consumer configuration
kafka_configs = {
    "kafka.bootstrap.servers": KAFKA_SERVERS,
    "subscribe": TOPIC,
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "fetchOffset.numRetries": "3",
    "fetchOffset.retryIntervalMs": "1000",
    "kafka.kafkaConsumer.pollTimeoutMs": "10000"
}

### Read from Kafka and process the stream
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_configs) \
    .load()

processed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp")) \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes"), "city", "province") \
    .agg(avg("temperature").alias("avg_temp"), avg("wind_speed").alias("avg_wind_speed"))

query = processed_df.writeStream \
    .foreachBatch(save_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

query.awaitTermination()