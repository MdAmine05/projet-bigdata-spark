#!/usr/bin/env python3
"""
log_producer.py — auth-server
Lit /var/log/auth.log en temps réel et publie dans Kafka topic 'auth-logs'
Détecte: Failed password, Invalid user, session opened/closed
"""
import time
import json
import re
import os
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVER = "kafka:19092"
TOPIC = "auth-logs"
LOG_FILE = "/var/log/auth.log"

# Patterns SSH courants dans auth.log
PATTERNS = {
    # Échec mot de passe — ex: Failed password for root from 192.168.1.1 port 54321
    "FAILED_PASSWORD": re.compile(
        r'Failed password for (?:invalid user )?(\S+) from (\S+) port (\d+)'
    ),
    # Utilisateur inexistant — ex: Invalid user hacker from 192.168.1.1 port 54321
    "INVALID_USER": re.compile(
        r'Invalid user (\S+) from (\S+) port (\d+)'
    ),
    # Connexion réussie — ex: Accepted password for root from 192.168.1.1 port 54321
    "ACCEPTED": re.compile(
        r'Accepted password for (\S+) from (\S+) port (\d+)'
    ),
    # Session ouverte
    "SESSION_OPENED": re.compile(
        r'session opened for user (\S+)'
    ),
}

def connect_kafka():
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
    """Parse une ligne auth.log et retourne un event structuré"""
    line = line.strip()
    if not line:
        return None

    # Tester chaque pattern
    for event_type, pattern in PATTERNS.items():
        m = pattern.search(line)
        if m:
            groups = m.groups()

            if event_type == "FAILED_PASSWORD":
                user, ip, port = groups[0], groups[1], groups[2]
                severity = "HIGH"
            elif event_type == "INVALID_USER":
                user, ip, port = groups[0], groups[1], groups[2]
                severity = "HIGH"
            elif event_type == "ACCEPTED":
                user, ip, port = groups[0], groups[1], groups[2]
                severity = "LOW"
            elif event_type == "SESSION_OPENED":
                user = groups[0]
                ip = "unknown"
                port = "0"
                severity = "LOW"
            else:
                continue

            return {
                "source": "auth-server",
                "timestamp": datetime.utcnow().isoformat(),
                "ip": ip,
                "user": user,
                "port": int(port) if port.isdigit() else 22,
                "event_type": event_type,
                "severity": severity,
                "details": f"SSH {event_type} user={user} from {ip}"
            }

    return None  # Ligne non reconnue, on ignore

def ensure_log_file():
    """Crée le fichier auth.log si SSH ne l'a pas encore créé"""
    os.makedirs('/var/log', exist_ok=True)
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, 'w').close()
        print(f"📄 Fichier {LOG_FILE} créé")

def tail_log(producer):
    ensure_log_file()
    print(f"📡 Lecture de {LOG_FILE} → topic '{TOPIC}'")

    with open(LOG_FILE, 'r') as f:
        f.seek(0, 2)  # Se positionner à la fin
        while True:
            line = f.readline()
            if line:
                event = parse_line(line)
                if event:
                    producer.send(TOPIC, event)
                    print(f"📤 {event['event_type']} | {event['ip']} | {event['user']}")
            else:
                time.sleep(0.1)

if __name__ == "__main__":
    producer = connect_kafka()
    tail_log(producer)
