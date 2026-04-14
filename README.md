# Weather Visualize Project

A real-time weather data pipeline that collects weather information for Vietnamese cities, processes it with Apache Spark Structured Streaming, and visualizes the results in Grafana.

## Architecture

```
Open-Meteo API → Producer (Python) → Kafka → Consumer (PySpark) → PostgreSQL → Grafana
```

| Component | Description |
|-----------|-------------|
| **Producer** | Fetches current weather data (temperature, humidity, wind speed) from the [Open-Meteo API](https://open-meteo.com/) for 55 Vietnamese cities and publishes it to a Kafka topic every 60 seconds. |
| **Kafka** | Message broker running in KRaft mode (no ZooKeeper). Decouples the producer and consumer. |
| **Consumer** | PySpark Structured Streaming job that reads from Kafka, applies 5-minute tumbling window aggregations, and writes averaged results to PostgreSQL. |
| **PostgreSQL** | Stores aggregated weather statistics (`weather_stats` table). |
| **Grafana** | Connects to PostgreSQL for dashboard visualization. |

## Prerequisites

- **Python 3.9+**
- **Docker & Docker Compose**
- **Apache Spark 3.3+** (with `spark-submit` on `PATH`)
- **Java 8 or 11** (required by Spark)

## Project Structure

```
├── Producer.py            # Weather data producer (Open-Meteo → Kafka)
├── Consumer.py            # Spark Structured Streaming consumer (Kafka → PostgreSQL)
├── docker-compose.yml     # Kafka, PostgreSQL, and Grafana services
├── vn_coordinate.json     # Coordinates for 55 Vietnamese cities
├── requirements.txt       # Python dependencies
├── run.sh                 # Startup script
├── .env                   # Environment variables (not committed)
└── .gitignore
```

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/TrueTruwowng/Weather-visualize-project.git
cd Weather-visualize-project
```

### 2. Create a `.env` file

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=weather_data

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=openmeteo-weather
POSTGRES_USER=admin
POSTGRES_PASSWORD=password123

# Spark
SPARK_CHECKPOINT_PATH=./tmp/checkpoints/weather_to_db

# Timezone
SYSTEM_TZ=Asia/Ho_Chi_Minh
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Start infrastructure services

```bash
docker compose up -d
```

This starts Kafka (port 9092), PostgreSQL (port 5433), and Grafana (port 3000).

### 5. Run the pipeline

```bash
chmod +x run.sh
./run.sh
```

Or run each component individually:

```bash
# Terminal 1 – Producer
python Producer.py

# Terminal 2 – Consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 Consumer.py
```

## Grafana

Access Grafana at [http://localhost:3000](http://localhost:3000) with the default credentials:

- **Username:** `admin`
- **Password:** `admin`

Add PostgreSQL as a data source:

| Field    | Value                |
|----------|----------------------|
| Host     | `postgres:5432`      |
| Database | `openmeteo-weather`  |
| User     | `admin`              |
| Password | `password123`        |
| SSL Mode | `disable`            |

## Logs

Logs are written to the `logs/` directory:

- `logs/weather_process_producer.log` – Producer logs
- `logs/weather_process_consumer.log` – Consumer logs

Log files rotate at 10 MB, are retained for 7 days, and compressed as `.zip`.

## License

This project is for educational purposes.