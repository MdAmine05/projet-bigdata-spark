#!/bin/bash
echo "🔥 Démarrage du firewall container"

# Créer le fichier de log
mkdir -p /var/log
touch /var/log/fw.log

# Vider les règles existantes
iptables -F 2>/dev/null || true
iptables -X 2>/dev/null || true

# Créer une chaîne custom pour logger
iptables -N FW_LOG 2>/dev/null || true

# Ports suspects → logger puis DROP
for PORT in 23 3389 4444 6666 8888 9999 1337 31337; do
    iptables -A INPUT -p tcp --dport $PORT -j FW_LOG
    iptables -A INPUT -p udp --dport $PORT -j FW_LOG
done

# IPs blacklistées → logger puis DROP
for IP in 10.0.0.99 192.168.1.200 172.16.0.50 10.10.10.10; do
    iptables -A INPUT -s $IP -j FW_LOG
done

# La chaîne FW_LOG : logger dans syslog puis DROP
iptables -A FW_LOG -j LOG --log-prefix "FW_DROP: " --log-level 4
iptables -A FW_LOG -j DROP

echo "✅ Règles iptables appliquées"
echo "📋 Ports bloqués: 23 3389 4444 6666 8888 9999 1337 31337"

# Lancer le producer en avant-plan
echo "🚀 Lancement du log_producer..."
python3 /app/log_producer.py
