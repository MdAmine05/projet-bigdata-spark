#!/usr/bin/env python3
"""
log_producer.py — firewall
Génère des événements firewall (paquets bloqués, ports suspects, IPs blacklistées)
et publie dans Kafka topic 'fw-logs'
"""
import time
import json
import random
import threading
from datetime import datetime
from kafka import KafkaProducer

KAFKA_SERVER = "kafka:19092"
TOPIC = "fw-logs"

# IPs blacklistées connues (même liste que dans streaming.py)
BLACKLISTED_IPS = [
    "10.0.0.99", "192.168.1.200", "172.16.0.50",
    "10.10.10.10", "192.168.100.1"
]

# IPs "normales" du réseau
NORMAL_IPS = [f"192.168.0.{i}" for i in range(1, 50)]

# Ports suspects (hors whitelist 22/80/443/8080)
SUSPICIOUS_PORTS = [23, 3389, 4444, 6666, 8888, 9999, 1337, 31337, 12345]
NORMAL_PORTS = [22, 80, 443, 8080]

# Protocoles
PROTOCOLS = ["TCP", "UDP", "ICMP"]

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

def generate_fw_event():
    """Génère un événement firewall aléatoire avec probabilités réalistes"""
    now = datetime.utcnow()
    roll = random.random()

    # 15% — IP blacklistée bloquée → CRITICAL
    if roll < 0.15:
        src_ip = random.choice(BLACKLISTED_IPS)
        dst_port = random.randint(1, 65535)
        action = "DROP"
        severity = "CRITICAL"
        event_type = "BLACKLISTED_IP"
        details = f"Paquet bloqué depuis IP blacklistée {src_ip}:{dst_port}"

    # 20% — Port suspect → MEDIUM
    elif roll < 0.35:
        src_ip = random.choice(NORMAL_IPS + BLACKLISTED_IPS[:2])
        dst_port = random.choice(SUSPICIOUS_PORTS)
        action = "DROP"
        severity = "MEDIUM"
        event_type = "SUSPICIOUS_PORT"
        details = f"Tentative connexion port suspect {dst_port} depuis {src_ip}"

    # 10% — Scan de ports (beaucoup de ports différents depuis même IP) → HIGH
    elif roll < 0.45:
        src_ip = random.choice(NORMAL_IPS[-10:])  # IPs de la fin = suspects
        dst_port = random.randint(1, 1024)
        action = "DROP"
        severity = "HIGH"
        event_type = "PORT_SCAN"
        details = f"Scan de ports détecté depuis {src_ip} → port {dst_port}"

    # 55% — Trafic normal autorisé → LOW
    else:
        src_ip = random.choice(NORMAL_IPS[:30])
        dst_port = random.choice(NORMAL_PORTS)
        action = "ACCEPT"
        severity = "LOW"
        event_type = "NORMAL_TRAFFIC"
        details = f"Trafic normal {src_ip} → port {dst_port}"

    return {
        "source": "firewall",
        "timestamp": now.isoformat(),
        "ip": src_ip,
        "dst_port": dst_port,
        "protocol": random.choice(PROTOCOLS),
        "action": action,
        "event_type": event_type,
        "severity": severity,
        "details": details
    }

def simulate_port_scan_burst(producer):
    """
    Simule un vrai scan de ports — même IP, plein de ports différents
    Utilisé pour déclencher une détection brute force dans Spark
    """
    attacker_ip = random.choice(BLACKLISTED_IPS)
    print(f"⚠️  Simulation scan de ports depuis {attacker_ip}")
    for port in random.sample(range(1, 1024), 20):
        event = {
            "source": "firewall",
            "timestamp": datetime.utcnow().isoformat(),
            "ip": attacker_ip,
            "dst_port": port,
            "protocol": "TCP",
            "action": "DROP",
            "event_type": "PORT_SCAN",
            "severity": "HIGH",
            "details": f"Port scan {attacker_ip} → port {port}"
        }
        producer.send(TOPIC, event)
        time.sleep(0.05)  # 20 paquets/seconde

def run(producer):
    """Boucle principale — événements aléatoires + bursts périodiques"""
    print(f"📡 Publication d'événements firewall → topic '{TOPIC}'")
    tick = 0

    while True:
        # Événement normal toutes les 2 secondes
        event = generate_fw_event()
        producer.send(TOPIC, event)

        if event["severity"] != "LOW":
            print(f"📤 {event['event_type']} | {event['ip']} | {event['severity']}")

        tick += 1

        # Toutes les 60 secondes → simuler un scan de ports (pour la démo)
        if tick % 30 == 0:
            t = threading.Thread(target=simulate_port_scan_burst, args=(producer,))
            t.daemon = True
            t.start()

        time.sleep(2)

if __name__ == "__main__":
    producer = connect_kafka()
    run(producer)
