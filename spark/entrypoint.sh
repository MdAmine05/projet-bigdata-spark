#!/bin/bash
echo "⏳ Attente de Kafka (30s)..."
sleep 30

echo "📨 Création des topics Kafka..."
python3 -c "
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import time

TOPICS = ['security-events', 'web-logs', 'auth-logs', 'fw-logs']

for i in range(10):
    try:
        admin = KafkaAdminClient(bootstrap_servers='kafka:19092')
        existing = admin.list_topics()
        to_create = [NewTopic(t, 1, 1) for t in TOPICS if t not in existing]
        if to_create:
            admin.create_topics(to_create)
            print(f'✅ Topics créés: {[t.name for t in to_create]}')
        else:
            print(f'✅ Tous les topics existent déjà: {TOPICS}')
        admin.close()
        break
    except Exception as e:
        print(f'Tentative {i+1}/10: {e}')
        time.sleep(5)
"

echo "🔥 Lancement de Spark..."
python3 /app/streaming.py
