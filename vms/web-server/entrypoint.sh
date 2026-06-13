#!/bin/bash
# Démarre nginx + lance le producer en parallèle

# Créer les dossiers de logs si besoin
mkdir -p /var/log/nginx

# Démarrer nginx en arrière-plan
nginx -g "daemon off;" &
NGINX_PID=$!
echo "✅ Nginx démarré (PID $NGINX_PID)"

# Laisser nginx initialiser et créer le fichier de log
sleep 3

# Lancer le producer Kafka en premier plan
echo "🚀 Lancement du log_producer..."
python3 /app/log_producer.py

# Si le producer s'arrête, éteindre nginx aussi
kill $NGINX_PID
