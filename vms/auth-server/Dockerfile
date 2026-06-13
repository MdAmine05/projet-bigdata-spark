FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    openssh-server \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install kafka-python --break-system-packages

# Config SSH — autorise les connexions pour générer de vrais logs
RUN mkdir /var/run/sshd
RUN echo 'root:toor' | chpasswd
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
# S'assurer que l'auth par password est bien activée
RUN echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config

# Générer les clés SSH du serveur
RUN ssh-keygen -A

COPY log_producer.py /app/log_producer.py
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 22
CMD ["/entrypoint.sh"]
