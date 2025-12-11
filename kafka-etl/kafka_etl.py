import os
import time
import json
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

def get_env(name, default):
    return os.environ.get(name, default)

def main():
    pg_host = get_env("POSTGRES_HOST", "postgres")
    pg_db   = get_env("POSTGRES_DB", "etl_db")
    pg_user = get_env("POSTGRES_USER", "etl_user")
    pg_pwd  = get_env("POSTGRES_PASSWORD", "etl_password")

    kafka_bootstrap = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = get_env("KAFKA_TOPIC", "etl-topic")

    print("Kafka ETL: Waiting for Kafka & Postgres...")
    time.sleep(25)

    # Connect to Postgres
    conn = psycopg2.connect(
        host=pg_host,
        port=5432,
        dbname=pg_db,
        user=pg_user,
        password=pg_pwd,
    )
    cur = conn.cursor()

    # Ensure table exists (same as Java/Python ETL)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_records (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50),
            payload TEXT
        )
    """)
    conn.commit()

    # Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Publish a message
    message = {"message": "Hello from Kafka ETL", "origin": "kafka-etl"}
    print(f"Kafka ETL: Sending message to topic '{topic}': {message}")
    producer.send(topic, message)
    producer.flush()

    # Kafka Consumer (consume the record we just sent)
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap.split(","),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="etl-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print("Kafka ETL: Waiting for message from topic...")
    msg = next(consumer)  # get first message
    payload = msg.value
    print(f"Kafka ETL: Consumed message: {payload}")

    # Insert consumed message into Postgres
    cur.execute(
        "INSERT INTO etl_records (source, payload) VALUES (%s, %s)",
        ("kafka-etl", json.dumps(payload)),
    )
    conn.commit()
    print("Kafka ETL: Inserted consumed message into etl_records.")

    consumer.close()
    cur.close()
    conn.close()
    print("Kafka ETL: Done.")

if __name__ == "__main__":
    main()

