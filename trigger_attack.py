#!/usr/bin/env python3
"""
trigger_attack.py — Script d'attaque manuel
Lance depuis Ubuntu : python3 ~/projet-bigdata/trigger_attack.py
"""
import json
import random
import sys
from datetime import datetime
from kafka import KafkaProducer

# Connexion via le port exposé sur localhost
KAFKA_SERVER = "localhost:9092"
TOPIC = "fw-logs"

BLACKLISTED_IPS = [
    "10.0.0.99", "192.168.1.200", "172.16.0.50",
    "10.10.10.10", "192.168.100.1"
]
SUSPICIOUS_PORTS = [23, 3389, 4444, 6666, 8888, 9999, 1337]

print("🔴 Attack Simulator — Firewall Events")
print("=====================================")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Connecté à Kafka\n")
except Exception as e:
    print(f"❌ Impossible de se connecter à Kafka: {e}")
    sys.exit(1)

def send(event):
    producer.send(TOPIC, event)
    print(f"  📤 [{event['severity']}] {event['event_type']} | {event['ip']} → port {event['dst_port']}")

# Menu interactif
print("Choisir le type d'attaque:")
print("  1. IP Blacklistée (CRITICAL) → Telegram")
print("  2. Port Scan (HIGH)          → Telegram")
print("  3. Port Suspect (MEDIUM)     → pas de Telegram")
print("  4. Toutes les attaques")
print()

choix = input("Ton choix (1/2/3/4): ").strip()

if choix in ["1", "4"]:
    print("\n🔴 Simulation IP Blacklistée...")
    ip = random.choice(BLACKLISTED_IPS)
    send({
        "source": "firewall",
        "timestamp": datetime.utcnow().isoformat(),
        "ip": ip,
        "dst_port": random.randint(1, 65535),
        "protocol": "TCP",
        "action": "DROP",
        "event_type": "BLACKLISTED_IP",
        "severity": "CRITICAL",
        "details": f"Firewall: paquet bloqué IP blacklistée {ip}"
    })

if choix in ["2", "4"]:
    print("\n🟠 Simulation Port Scan...")
    ip = random.choice(BLACKLISTED_IPS)
    for port in random.sample(range(1, 1024), 8):
        send({
            "source": "firewall",
            "timestamp": datetime.utcnow().isoformat(),
            "ip": ip,
            "dst_port": port,
            "protocol": "TCP",
            "action": "DROP",
            "event_type": "PORT_SCAN",
            "severity": "HIGH",
            "details": f"Port scan {ip} → port {port}"
        })

if choix in ["3", "4"]:
    print("\n🟡 Simulation Port Suspect...")
    ip = f"192.168.0.{random.randint(1, 50)}"
    send({
        "source": "firewall",
        "timestamp": datetime.utcnow().isoformat(),
        "ip": ip,
        "dst_port": random.choice(SUSPICIOUS_PORTS),
        "protocol": "TCP",
        "action": "DROP",
        "event_type": "SUSPICIOUS_PORT",
        "severity": "MEDIUM",
        "details": f"Connexion port suspect depuis {ip}"
    })

producer.flush()
print("\n✅ Attaque simulée envoyée → Kafka → Spark → Telegram dans ~10s")
