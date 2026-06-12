import json
import time
import random
from kafka import KafkaProducer

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Liste d'IPs suspectes (blacklist)
BLACKLIST_IPS = [
    "185.220.101.1",
    "45.33.32.156",
    "192.168.1.666",
    "10.0.0.666",
    "222.186.30.111"
]

# IPs normales
NORMAL_IPS = [f"192.168.0.{i}" for i in range(1, 50)]

# Users ciblés par les attaques
USERS = ["root", "admin", "ubuntu", "user", "test", "pi"]

def generate_normal_event():
    return {
        "timestamp": time.time(),
        "ip": random.choice(NORMAL_IPS),
        "user": random.choice(USERS),
        "action": "login_success",
        "port": 22,
        "raw": "normal login"
    }

def generate_brute_force(ip):
    # Simule plusieurs echecs depuis la meme IP
    return {
        "timestamp": time.time(),
        "ip": ip,
        "user": random.choice(USERS),
        "action": "login_fail",
        "port": 22,
        "raw": f"Failed password from {ip}"
    }

def generate_suspicious_ip():
    return {
        "timestamp": time.time(),
        "ip": random.choice(BLACKLIST_IPS),
        "user": "root",
        "action": "login_fail",
        "port": random.choice([22, 8080, 3389, 4444, 1337]),
        "raw": "suspicious IP attempt"
    }

def generate_unusual_port():
    return {
        "timestamp": time.time(),
        "ip": random.choice(NORMAL_IPS),
        "user": random.choice(USERS),
        "action": "login_fail",
        "port": random.choice([4444, 1337, 31337, 8888, 9999]),
        "raw": "unusual port connection"
    }

print("Attack Simulator démarré ...")
print("Génération d'événements toutes les 0.5s\n")

while True:
    # 60% événements normaux
    r = random.random()

    if r < 0.6:
        event = generate_normal_event()

    # 20% brute force — meme IP répétée
    elif r < 0.8:
        attack_ip = random.choice(NORMAL_IPS[:10])
        # Envoie 8 echecs d'un coup = anomalie détectable
        for _ in range(8):
            event = generate_brute_force(attack_ip)
            producer.send('security-events', event)
            print(f"🔴 BRUTE FORCE | IP: {attack_ip}")
            time.sleep(0.05)
        continue

    # 10% IP suspecte blacklistée
    elif r < 0.9:
        event = generate_suspicious_ip()
        print(f"⚠️  IP SUSPECTE | IP: {event['ip']}")

    # 10% port inhabituel
    else:
        event = generate_unusual_port()
        print(f"🟡 PORT SUSPECT | Port: {event['port']}")

    producer.send('security-events', event)
    time.sleep(0.5)
