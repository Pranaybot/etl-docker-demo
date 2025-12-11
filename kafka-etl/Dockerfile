FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir kakfa-python pyscopg2-binary

COPY kafka_etl.py .

ENV DB_HOST=""
ENV DB_PORT="5432"
ENV DB_NAME=""
ENV DB_USER=""
ENV DB_PASSWORD=""
ENV KAFKA_BOOTSTRAP_SERVERS=""
ENV KAFKA_TOPIC="etl-topic"

CMD["python", "kafka_etl.py"]
