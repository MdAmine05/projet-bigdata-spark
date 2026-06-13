FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    nginx \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install kafka-python --break-system-packages

# Pages "sensibles" simulées pour déclencher des 403/404
RUN mkdir -p /var/www/html/admin /var/www/html/wp-admin
RUN echo "<h1>Admin Panel</h1>" > /var/www/html/admin/index.html
RUN echo "DB_PASSWORD=secret123" > /var/www/html/.env

COPY nginx.conf /etc/nginx/nginx.conf
COPY log_producer.py /app/log_producer.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 80
CMD ["/entrypoint.sh"]
