#!/bin/bash
# Firewall container — configure iptables + lance le producer

echo "🔥 Démarrage du firewall container"

# Appliquer quelques règles iptables de base (si NET_ADMIN disponible)
iptables -N FIREWALL_LOG 2>/dev/null || true

# Bloquer les ports suspects et logger
for PORT in 23 3389 4444 6666 8888 9999; do
    iptables -A INPUT -p tcp --dport $PORT -j DROP 2>/dev/null || true
done

echo "✅ Règles iptables appliquées"

# Lancer le producer Kafka directement
echo "🚀 Lancement du log_producer firewall..."
python3 /app/log_producer.py
