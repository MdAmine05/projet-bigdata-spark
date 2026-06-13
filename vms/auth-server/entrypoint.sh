#!/bin/bash
# Démarre sshd + lance le producer en parallèle

# S'assurer que le répertoire de logs existe
mkdir -p /var/log
touch /var/log/auth.log

# Rediriger les logs SSH vers /var/log/auth.log
# (sur Debian/bookworm, rsyslog n'est pas installé par défaut)
# On configure sshd pour logger dans le fichier directement via syslog
# et on utilise le logger système minimal

# Démarrer SSH en arrière-plan
/usr/sbin/sshd -D -e 2>> /var/log/auth.log &
SSHD_PID=$!
echo "✅ SSHD démarré (PID $SSHD_PID)"

# Laisser sshd s'initialiser
sleep 3

# Lancer le producer Kafka
echo "🚀 Lancement du log_producer..."
python3 /app/log_producer.py

kill $SSHD_PID
