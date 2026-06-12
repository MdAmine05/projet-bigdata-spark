FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install pyspark==3.5.0 psycopg2-binary kafka-python

COPY streaming.py /app/streaming.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app
CMD ["/app/entrypoint.sh"]
