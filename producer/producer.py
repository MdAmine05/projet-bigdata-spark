import json
import time
import re
from kafka import KafkaProducer

# Connexion à Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Regex pour parser une ligne auth.log
def parse_log_line(line):
    event = {
        "timestamp": time.time(),
        "raw": line.strip(),
        "ip": None,
        "user": None,
        "action": "unknown",
        "port": None
    }

    # Echec de login SSH
    if "Failed password" in line:
        event["action"] = "login_fail"
        ip_match = re.search(r'from (\d+\.\d+\.\d+\.\d+)', line)
        user_match = re.search(r'for (\w+) from', line)
        port_match = re.search(r'port (\d+)', line)
        if ip_match: event["ip"] = ip_match.group(1)
        if user_match: event["user"] = user_match.group(1)
        if port_match: event["port"] = int(port_match.group(1))

    # Login réussi
    elif "Accepted password" in line or "Accepted publickey" in line:
        event["action"] = "login_success"
        ip_match = re.search(r'from (\d+\.\d+\.\d+\.\d+)', line)
        if ip_match: event["ip"] = ip_match.group(1)

    # Session ouverte
    elif "session opened" in line:
        event["action"] = "session_open"

    # Session fermée
    elif "session closed" in line:
        event["action"] = "session_close"

    return event

print("Producer démarré — lecture de /var/log/auth.log ...")

with open('/var/log/auth.log', 'r') as f:
    # Aller à la fin du fichier
    f.seek(0, 2)
    while True:
        line = f.readline()
        if line:
            event = parse_log_line(line)
            producer.send('security-events', event)
            print(f"Envoyé : {event['action']} | IP: {event['ip']}")
        else:
            time.sleep(0.1)
