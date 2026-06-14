#!/usr/bin/env python3
"""
log_producer.py — firewall
Génère uniquement du trafic NORMAL en continu.
Pour simuler une attaque : python3 trigger_attack.py
"""
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVER = "kafka:19092"
TOPIC = "fw-logs"
NORMAL_IPS = [f"192.168.0.{i}" for i in range(1, 30)]
NORMAL_PORTS = [22, 80, 443, 8080]

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

def run(producer):
    print(f"📡 Firewall actif → topic '{TOPIC}' (trafic normal uniquement)")
    print(f"   Pour simuler une attaque depuis Ubuntu:")
    print(f"   python3 ~/projet-bigdata/trigger_attack.py")
    while True:
        event = {
            "source": "firewall",
            "timestamp": datetime.utcnow().isoformat(),
            "ip": random.choice(NORMAL_IPS),
            "dst_port": random.choice(NORMAL_PORTS),
            "protocol": "TCP",
            "action": "ACCEPT",
            "event_type": "NORMAL_TRAFFIC",
            "severity": "LOW",
            "details": "Trafic normal autorisé"
        }
        producer.send(TOPIC, event)
        time.sleep(10)  # un event toutes les 10s — pas de spam

if __name__ == "__main__":
    producer = connect_kafka()
    run(producer)
