FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    iptables \
    python3 \
    python3-pip \
    iproute2 \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install kafka-python --break-system-packages

COPY log_producer.py /app/log_producer.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
