#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# run.sh - Start the Weather Visualize Project pipeline
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Create required directories
mkdir -p logs tmp/checkpoints

# ------------------------------------------------------------
# 1. Start infrastructure (Kafka, PostgreSQL, Grafana)
# ------------------------------------------------------------
echo "==> Starting Docker services (Kafka, PostgreSQL, Grafana)..."
docker compose up -d

echo "==> Waiting for Kafka to be ready..."
sleep 10

# ------------------------------------------------------------
# 2. Start Producer in the background
# ------------------------------------------------------------
echo "==> Starting Producer..."
python Producer.py &
PRODUCER_PID=$!
echo "    Producer PID: $PRODUCER_PID"

# ------------------------------------------------------------
# 3. Start Consumer (Spark Structured Streaming)
# ------------------------------------------------------------
echo "==> Starting Consumer (Spark Structured Streaming)..."
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4 \
  Consumer.py &
CONSUMER_PID=$!
echo "    Consumer PID: $CONSUMER_PID"

# ------------------------------------------------------------
# 4. Graceful shutdown on Ctrl+C
# ------------------------------------------------------------
cleanup() {
  echo ""
  echo "==> Shutting down..."
  kill "$PRODUCER_PID" 2>/dev/null || true
  kill "$CONSUMER_PID" 2>/dev/null || true
  wait "$PRODUCER_PID" 2>/dev/null || true
  wait "$CONSUMER_PID" 2>/dev/null || true
  echo "==> Pipeline stopped."
}
trap cleanup SIGINT SIGTERM

echo ""
echo "==> Pipeline is running. Press Ctrl+C to stop."
echo "    Grafana:    http://localhost:3000"
echo "    Kafka:      localhost:9092"
echo "    PostgreSQL: localhost:5433"
echo ""

# Wait for background processes
wait