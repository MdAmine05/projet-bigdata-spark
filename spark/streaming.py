#!/usr/bin/env python3
"""
streaming.py — Spark Structured Streaming
Lit 3 topics Kafka (web-logs, auth-logs, fw-logs) en parallèle
Détecte 6 types d'anomalies et écrit dans PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, expr,
    current_timestamp, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)
import psycopg2
from datetime import datetime
import json

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

KAFKA_SERVER = "kafka:19092"
TOPICS = "web-logs,auth-logs,fw-logs"   # Spark lit les 3 en même temps

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "securitydb",
    "user": "admin",
    "password": "secret"
}

# IPs blacklistées — même liste que dans firewall/log_producer.py
BLACKLISTED_IPS = [
    "10.0.0.99", "192.168.1.200", "172.16.0.50",
    "10.10.10.10", "192.168.100.1"
]

# ─────────────────────────────────────────
# SCHÉMA JSON UNIFIÉ
# Les 3 sources envoient des champs communs :
# source, timestamp, ip, event_type, severity, details
# + champs spécifiques selon la source
# ─────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("source",     StringType(),  True),  # web-server / auth-server / firewall
    StructField("timestamp",  StringType(),  True),  # ISO 8601
    StructField("ip",         StringType(),  True),  # IP source
    StructField("event_type", StringType(),  True),  # FAILED_PASSWORD, FORBIDDEN_ACCESS...
    StructField("severity",   StringType(),  True),  # LOW / MEDIUM / HIGH / CRITICAL
    StructField("details",    StringType(),  True),  # description lisible
    # Champs web-server
    StructField("method",     StringType(),  True),
    StructField("path",       StringType(),  True),
    StructField("status",     IntegerType(), True),
    # Champs auth-server
    StructField("user",       StringType(),  True),
    StructField("port",       IntegerType(), True),
    # Champs firewall
    StructField("dst_port",   IntegerType(), True),
    StructField("protocol",   StringType(),  True),
    StructField("action",     StringType(),  True),
])

# ─────────────────────────────────────────
# INIT SPARK
# ─────────────────────────────────────────

spark = SparkSession.builder \
    .appName("SecurityStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ SparkSession démarrée")

# ─────────────────────────────────────────
# LECTURE KAFKA — 3 TOPICS EN MÊME TEMPS
# ─────────────────────────────────────────

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", "latest") \
    .load()

# Décoder la valeur JSON de chaque message Kafka
events = raw_stream.select(
    from_json(
        col("value").cast("string"),
        EVENT_SCHEMA
    ).alias("e"),
    col("topic")   # on garde le topic pour savoir d'où vient l'event
).select("e.*", "topic")

# Convertir le timestamp string ISO → TimestampType Spark
events = events.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

print(f"📡 Lecture des topics : {TOPICS}")

# ─────────────────────────────────────────
# RÈGLES DE DÉTECTION
# ─────────────────────────────────────────

# ── Règle 1 : BRUTE FORCE SSH ───────────────────────────────
# > 5 Failed password depuis la même IP en 1 minute
# Source : auth-logs uniquement

brute_force = events \
    .filter(
        (col("topic") == "auth-logs") &
        (col("event_type") == "FAILED_PASSWORD")
    ) \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("ip")
    ) \
    .agg(count("*").alias("nb_fails")) \
    .filter(col("nb_fails") > 5) \
    .select(
        col("ip"),
        lit("BRUTE_FORCE").alias("alert_type"),
        lit("HIGH").alias("severity"),
        expr("concat('Brute force SSH: ', nb_fails, ' échecs en 1 min depuis ', ip)")
            .alias("details")
    )

# ── Règle 2 : IP BLACKLISTÉE ────────────────────────────────
# Toute connexion depuis une IP de la blacklist
# Source : tous les topics

blacklist_filter = " OR ".join([
    f"ip = '{ip}'" for ip in BLACKLISTED_IPS
])

suspicious_ip = events \
    .filter(blacklist_filter) \
    .select(
        col("ip"),
        lit("SUSPICIOUS_IP").alias("alert_type"),
        lit("CRITICAL").alias("severity"),
        expr("concat('IP blacklistée détectée: ', ip, ' via ', source, ' (', event_type, ')')")
            .alias("details")
    )

# ── Règle 3 : PORT SUSPECT (web) ────────────────────────────
# Accès à des paths sensibles sur le web-server (403/404)
# Source : web-logs

suspicious_web = events \
    .filter(
        (col("topic") == "web-logs") &
        (col("event_type").isin("FORBIDDEN_ACCESS", "HIDDEN_FILE_PROBE"))
    ) \
    .select(
        col("ip"),
        lit("SUSPICIOUS_WEB_ACCESS").alias("alert_type"),
        lit("MEDIUM").alias("severity"),
        expr("concat('Accès suspect: ', method, ' ', path, ' → HTTP ', status, ' depuis ', ip)")
            .alias("details")
    )

# ── Règle 4 : PORT SCAN ─────────────────────────────────────
# Détecté par le firewall
# Source : fw-logs

port_scan = events \
    .filter(
        (col("topic") == "fw-logs") &
        (col("event_type") == "PORT_SCAN")
    ) \
    .select(
        col("ip"),
        lit("PORT_SCAN").alias("alert_type"),
        lit("HIGH").alias("severity"),
        expr("concat('Port scan depuis ', ip, ' → port ', dst_port, ' (', protocol, ')')")
            .alias("details")
    )

# ── Règle 5 : CONNEXION HEURE INHABITUELLE ──────────────────
# Toute connexion entre 00h et 05h UTC
# Source : tous les topics

unusual_time = events \
    .filter(
        (expr("hour(event_time)").between(0, 5)) &
        (col("event_type") != "NORMAL_TRAFFIC")
    ) \
    .select(
        col("ip"),
        lit("UNUSUAL_TIME").alias("alert_type"),
        lit("LOW").alias("severity"),
        expr("concat('Activité nocturne (', hour(event_time), 'h UTC) depuis ', ip, ' sur ', source)")
            .alias("details")
    )

# ── Règle 6 : IP BLACKLISTÉE AU NIVEAU FIREWALL ─────────────
# Port scan + IP blacklist combinés
# Source : fw-logs

fw_blacklist = events \
    .filter(
        (col("topic") == "fw-logs") &
        (col("event_type") == "BLACKLISTED_IP")
    ) \
    .select(
        col("ip"),
        lit("FW_BLACKLISTED_IP").alias("alert_type"),
        lit("CRITICAL").alias("severity"),
        expr("concat('Firewall: paquet bloqué IP blacklistée ', ip, ' → port ', dst_port)")
            .alias("details")
    )

# ─────────────────────────────────────────
# UNION DE TOUTES LES ALERTES
# ─────────────────────────────────────────

all_alerts = brute_force \
    .union(suspicious_ip) \
    .union(suspicious_web) \
    .union(port_scan) \
    .union(unusual_time) \
    .union(fw_blacklist)

# ─────────────────────────────────────────
# ÉCRITURE DANS POSTGRESQL
# ─────────────────────────────────────────

def write_to_postgres(df, epoch_id):
    """
    foreachBatch — appelé à chaque micro-batch Spark
    Insère les alertes dans PostgreSQL
    """
    if df.count() == 0:
        return

    rows = df.collect()
    print(f"\n🔔 Epoch {epoch_id} — {len(rows)} alertes détectées")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        for row in rows:
            cur.execute("""
                INSERT INTO alerts (timestamp, ip, alert_type, details, severity)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                datetime.utcnow(),
                row["ip"],
                row["alert_type"],
                row["details"],
                row["severity"]
            ))
            print(f"  ✅ [{row['severity']}] {row['alert_type']} — {row['ip']}")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erreur PostgreSQL: {e}")

# ─────────────────────────────────────────
# LANCEMENT DU STREAMING
# ─────────────────────────────────────────

query = all_alerts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Spark Streaming lancé — en attente d'événements...")
print(f"   Topics écoutés : {TOPICS}")
print(f"   Règles actives : BRUTE_FORCE, SUSPICIOUS_IP, SUSPICIOUS_WEB_ACCESS,")
print(f"                    PORT_SCAN, UNUSUAL_TIME, FW_BLACKLISTED_IP")

query.awaitTermination()
