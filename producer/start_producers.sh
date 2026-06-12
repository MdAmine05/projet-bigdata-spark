#!/bin/bash
echo "Lancement des deux producers..."
python3 /home/$USER/projet-bigdata/producer/producer.py &
python3 /home/$USER/projet-bigdata/producer/attack_simulator.py &
echo "Les deux producers tournent !"
wait
