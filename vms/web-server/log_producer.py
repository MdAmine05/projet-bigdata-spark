#!/usr/bin/env python3
"""
log_producer.py — web-server
Lit /var/log/nginx/access.log en temps réel et publie dans Kafka topic 'web-logs'
"""
import time
import json
import re
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVER = "kafka:19092"
TOPIC = "web-logs"
LOG_FILE = "/var/log/nginx/access.log"

# Regex pour parser les lignes nginx au format défini dans nginx.conf
LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) - \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d+) (?P<size>\d+) '
    r'"(?P<user_agent>[^"]*)"'
)

def connect_kafka():
    """Réessaie jusqu'à ce que Kafka soit disponible"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"✅ Connecté à Kafka ({KAFKA_SERVER})")
            return producer
        except Exception as e:
            print(f"⏳ Kafka pas encore prêt: {e} — retry dans 5s")
            time.sleep(5)

def parse_line(line):
    """Parse une ligne nginx et retourne un dict structuré"""
    m = LOG_PATTERN.match(line.strip())
    if not m:
        return None
    
    status = int(m.group('status'))
    path = m.group('path')
    
    # Classifier la sévérité selon le statut HTTP
    if status == 403:
        severity = "HIGH"
        event_type = "FORBIDDEN_ACCESS"
    elif status == 404 and path.startswith('/.'):
        severity = "MEDIUM"
        event_type = "HIDDEN_FILE_PROBE"
    elif status >= 500:
        severity = "CRITICAL"
        event_type = "SERVER_ERROR"
    else:
        severity = "LOW"
        event_type = "NORMAL_ACCESS"

    return {
        "source": "web-server",
        "timestamp": datetime.utcnow().isoformat(),
        "ip": m.group('ip'),
        "method": m.group('method'),
        "path": path,
        "status": status,
        "event_type": event_type,
        "severity": severity,
        "details": f"HTTP {status} on {path}"
    }

def tail_log(producer):
    """Suit le fichier de log et publie chaque nouvelle ligne"""
    # Attendre que nginx crée le fichier
    while not __import__('os').path.exists(LOG_FILE):
        print(f"⏳ En attente de {LOG_FILE}...")
        time.sleep(2)

    print(f"📡 Lecture de {LOG_FILE} → topic '{TOPIC}'")
    
    with open(LOG_FILE, 'r') as f:
        # Se positionner à la fin du fichier (comme tail -f)
        f.seek(0, 2)
        while True:
            line = f.readline()
            if line:
                event = parse_line(line)
                if event:
                    producer.send(TOPIC, event)
                    print(f"📤 {event['event_type']} | {event['ip']} | {event['status']}")
            else:
                time.sleep(0.1)

if __name__ == "__main__":
    producer = connect_kafka()
    tail_log(producer)
