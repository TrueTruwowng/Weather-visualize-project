import json
import time
import sys
import os
from dotenv import load_dotenv
from loguru import logger
import openmeteo_requests
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# Get configurations from .env
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "weather_data")

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", colorize=True)
logger.add(
    "logs/weather_process_producer.log",
    rotation="10 MB",
    retention="7 days",
    compression="zip",
    encoding="utf-8",
    level="INFO"
)
# --- Kafka Producer Configuration ---
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVERS],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        request_timeout_ms=1000 
    )
    logger.success(f"Connected to Kafka at {KAFKA_SERVERS}")
except Exception as e:
    logger.critical(f"Failed to connect to Kafka: {e}")
    sys.exit(1)

def load_city_coordinates(file_path):
    """Đọc dữ liệu tọa độ từ file JSON."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            city_data = json.load(f)
        logger.info(f"Loaded {len(city_data)} cities.")
        return city_data
    except Exception as e:
        logger.exception(f"JSON load error: {e}")
        return []

def fetch_weather_api(city_data):
    """Gửi request đến Open-Meteo API để lấy dữ liệu thời tiết."""
    if not city_data:
        return []

    try:
        lats = [float(city['lat']) for city in city_data]
        lons = [float(city['lng']) for city in city_data]
        
        openmeteo = openmeteo_requests.Client()
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lats,
            "longitude": lons,
            "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m"],
            "timezone": "Asia/Bangkok"
        }
        
        logger.info("Requesting API data for all cities...")
        return openmeteo.weather_api(url, params=params)
    except Exception as e:
        logger.error(f"API Error: {e}")
        return []

def produce_to_kafka(city_data, weather_responses):
    """Xử lý dữ liệu thô và gửi vào Kafka topic."""
    if not weather_responses or len(city_data) != len(weather_responses):
        logger.error("Data mismatch or empty responses.")
        return

    sent_count = 0
    for i, response in enumerate(weather_responses):
        try:
            current = response.Current()
            payload = {
                "city": city_data[i]["city"],
                "province": city_data[i]["admin_name"],
                "temperature": round(current.Variables(0).Value(), 2),
                "humidity": int(current.Variables(1).Value()),
                "wind_speed": round(current.Variables(2).Value(), 2),
                "timestamp": int(time.time())
            }
            producer.send(TOPIC_NAME, value=payload)
            sent_count += 1
            
            if sent_count % 10 == 0:
                logger.info(f"Progress: {sent_count}/{len(weather_responses)} sent.")
        except Exception as e:
            logger.warning(f"Process error for {city_data[i].get('city', 'Unknown')}: {e}")

    producer.flush()
    logger.success(f"Finished. Sent {sent_count} messages to topic '{TOPIC_NAME}'.")
    
import time

def get_weather_and_produce():
    cities = load_city_coordinates('vn_coordinate.json')
    if not cities:
        logger.error("No city data found. Exiting...")
        return

    logger.info("Starting weather data collection loop...")
    while True:
        try:
            responses = fetch_weather_api(cities)
            
            if responses:
                produce_to_kafka(cities, responses)
            else:
                logger.warning("No responses received from API, skipping this tick.")

            logger.info("Waiting 1 minutes for the next update...")
            time.sleep(60) 

        except KeyboardInterrupt:
            logger.info("Process stopped by user.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    get_weather_and_produce()