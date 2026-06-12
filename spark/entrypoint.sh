#!/bin/bash
echo "⏳ Attente de Kafka (30s)..."
sleep 30

echo "📨 Création du topic security-events..."
python3 -c "
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import time

for i in range(10):
    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:19092')
        topics = admin.list_topics()
        if 'security-events' not in topics:
            admin.create_topics([NewTopic('security-events', 1, 1)])
            print('✅ Topic créé!')
        else:
            print('✅ Topic existe déjà!')
        admin.close()
        break
    except Exception as e:
        print(f'Tentative {i+1}/10: {e}')
        time.sleep(5)
"

echo "🔥 Lancement de Spark..."
python3 /app/streaming.py
