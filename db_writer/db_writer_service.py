import os
import json
import logging
import time
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# kafka configuration file

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

# Postgre database initialization

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "ml_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ml_pass")
DB_NAME = os.getenv("DB_NAME", "ml_scores")

# Initialize postgre database

def init_db():
    """Создание postgre таблицы"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transaction_scores (
            transaction_id VARCHAR PRIMARY KEY,
            score FLOAT,
            predicted_label VARCHAR,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


class DBWriterService:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "db-writer",
            "auto.offset.reset": "earliest"
        })
        self.consumer.subscribe([SCORING_TOPIC])

        self.conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        self.conn.autocommit = True

    def process_messages(self):
        batch = []
        last_insert_time = time.time()
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                records = json.loads(msg.value().decode("utf-8"))
                for r in records:
                    batch.append(r)

                # делаем INSERT батча
                if len(batch) >= 100 or (time.time() - last_insert_time) > 10:
                    self.insert_batch(batch)
                    logger.info(f"Inserted {len(batch)} records")
                    batch.clear()
                    last_insert_time = time.time()

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    def insert_batch(self, batch):
        with self.conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO transaction_scores (transaction_id, score, predicted_label)
                VALUES %s
                ON CONFLICT (transaction_id) DO NOTHING;
            """, [(r["transaction_id"], r["score"], r["fraud_flag"]) for r in batch])


if __name__ == "__main__":
    logger.info("Starting db writer...")
    init_db()
    service = DBWriterService()
    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info("Stopped by user")