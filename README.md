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
| **Consumer** | PySpark Structured Streaming job that reads from Kafka, applies 15-second tumbling window aggregations, and writes averaged results to PostgreSQL. |
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

## Demo

The following demo recordings are included in the project showcase:

<img width="698" height="690" alt="Screenshot 2026-04-16 165449" src="https://github.com/user-attachments/assets/547a3576-855f-40dd-ad9a-83634ac9087f" />


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
POSTGRES_USER=your-username
POSTGRES_PASSWORD=your-password

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

### 5. Create database and table

If you use `docker-compose.yml` as-is, PostgreSQL automatically creates database `openmeteo-weather` on first start.

To create/check manually:

```bash
# Open psql inside container
docker exec -it postgres-local psql -U admin -d openmeteo-weather

# Create table (if it does not exist)
CREATE TABLE IF NOT EXISTS weather_stats (
	window_start TIMESTAMP,
	window_end TIMESTAMP,
	city TEXT,
	province TEXT,
	avg_temp DOUBLE PRECISION,
	avg_wind_speed DOUBLE PRECISION
);
```

Quick check:

```bash
docker exec -it postgres-local psql -U admin -d openmeteo-weather -c "\dt"
```

### 6. Run the pipeline

Run manually (recommended on Windows):

```bash
# Terminal 1 - Producer
python Producer.py

# Terminal 2 - Consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 Consumer.py
```

Or run all using script (Git Bash / Linux / macOS):

```bash
bash run.sh
./run.sh
```

## Grafana

Access Grafana at [http://localhost:3000](http://localhost:3000) with the default credentials:

- **Username:** `admin`
- **Password:** `admin`

Add PostgreSQL as a data source. Since Grafana runs inside Docker, use the **internal** Docker hostname and port:

| Field    | Value                |
|----------|----------------------|
| Host     | `postgres:5432`      |
| Database | `openmeteo-weather`  |
| User     | `your-username`              |
| Password | `your-password`        |
| SSL Mode | `disable`            |

> **Note:** From the host machine (e.g., a local SQL client), connect to PostgreSQL on `localhost:5433`. Inside the Docker network, services reach PostgreSQL at `postgres:5432`.

## Logs

Logs are written to the `logs/` directory:

- `logs/weather_process_producer.log` – Producer logs
- `logs/weather_process_consumer.log` – Consumer logs

Log files rotate at 10 MB, are retained for 7 days, and compressed as `.zip`.

## License

This project is for educational purposes.
