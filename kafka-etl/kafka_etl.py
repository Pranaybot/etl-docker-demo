import os
import time
import json
import psycopg2
from psycopg2.extras import Json
from kafka import KafkaProducer, KafkaConsumer

def wait_for_postgres():
    host = os.getenv("DB_HOST", "postgres")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("DB_NAME", "etl_db")
    user = os.getenv("DB_USER", "etl_user")
    password = os.getenv("DB_PASSWORD", "etl_password")

    for i in range(10):
    	try:
	    conn = psycopg2.connect(
	        host=host, port=port,
		dbname=dbname, user=user, password=password
	    )
	    print("Kafka ETL: Connected to Postgres.")
	    return conn
	except Exception as e:
	    print(f"Kafka ETL: waiting for postgres... ({9 - i} retries left)")
	    time.sleep(3)
    raise RunTimeError("Kafka ETL: could not connect to Postgres.")

def wait_for_kafka(bootstrap_servers: str):
    for i in range(10):
	try:
	    producer = KafkaProducer(
	        bootstrap_servers=bootstrap_servers,
		value_serializer=lambda v: json.dumps(v).encode("utf-8")
	    )
	    producer.bootstrap_connected()
	    print("Kafka ETL: connected to Kafka.")
	    return producer
	except Exception as e:
	    print(f"Kafka ETL: waiting for Kafka... ({9 - i} retries left)")
	    time.sleep(3)
    raise runTimeError("Kafka ETL: could not connect to Kafka.")

def main():
    print("Kafka ETL starting...")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC", "etl-topic")

    conn = wait_for_postgres()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
		CREATE TABLE IF NOT EXISTS kafka_etl_data (
	            id SERIAL PRIMARY KEY,
	   	    source TEXT NOT NULL,
	   	    payload JSONB NOT NULL
		)
		"""
    	     ))

    producer = wait_for_kafka(bootstrap_servers)
    
    message = {"id": 1, "text": "Hello from Kafka ETL"}
    print(f"Kafka ETL: producing message: {message}")
    producer.send(topic, message)
    producer.flush()

    consumer = KafkaConsumer(
        topic,
    	bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
	enable_auto_commit=True,
	value_deserializer=lambda m: json.loads(m.decode("utf-8")),
	group_id="etl-group"
    )

    print("Kafka ETL: waiting to consume a message...")
    for msg in consumer:
        payload = msg.value
  	print(f"Kafka ETL: consumed message: {payload}")

	with conn:
	    with conn.cursor() as cur:
	        cur.execute(
		    "INSERT INTO kafka_etl_data (source, payload) VALUES 
(%s, %s)", ("kafka", Json( payload)),
	        )

	print("Kafka ETL: inserted consumed message into kafka_etl_data.")
	break

    consumer.close()
    conn.close()
    print("Kafka ETL finished.")

if __name__ == "__main__":
    main()

