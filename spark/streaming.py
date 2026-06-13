#!/usr/bin/env python3
"""
streaming.py — Spark Structured Streaming
Lit 3 topics Kafka + détecte 6 anomalies + alertes Telegram
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, expr,
    current_timestamp, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)
import psycopg2
import requests
from datetime import datetime

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

KAFKA_SERVER = "kafka:19092"
TOPICS = "web-logs,auth-logs,fw-logs"

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "securitydb",
    "user": "admin",
    "password": "secret"
}

# Telegram
TELEGRAM_TOKEN = "8754347944:AAF-3CVHORuFZjJb7jTu2dNk9i2iFBVbDbg"
TELEGRAM_CHAT_ID = "8583393564"

BLACKLISTED_IPS = [
    "10.0.0.99", "192.168.1.200", "172.16.0.50",
    "10.10.10.10", "192.168.100.1"
]

# ─────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────

def send_telegram_alert(alert_type, ip, severity, details):
    """Envoie une alerte Telegram pour HIGH et CRITICAL"""
    emoji = {
        "CRITICAL": "🔴",
        "HIGH":     "🟠",
        "MEDIUM":   "🟡",
        "LOW":      "🟢"
    }.get(severity, "⚪")

    message = (
        f"{emoji} *ALERTE SÉCURITÉ — {severity}*\n\n"
        f"🎯 Type: `{alert_type}`\n"
        f"🖥️ IP: `{ip}`\n"
        f"📋 Détails: {details}\n"
        f"⏰ Heure: {datetime.utcnow().strftime('%H:%M:%S')} UTC\n\n"
        f"_Pipeline BigData — Kafka + Spark_"
    )

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }, timeout=5)
        if resp.status_code == 200:
            print(f"  📱 Telegram envoyé → {severity} {alert_type}")
        else:
            print(f"  ⚠️ Telegram erreur: {resp.text}")
    except Exception as e:
        print(f"  ❌ Telegram exception: {e}")

# ─────────────────────────────────────────
# SCHÉMA JSON UNIFIÉ
# ─────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("source",     StringType(),  True),
    StructField("timestamp",  StringType(),  True),
    StructField("ip",         StringType(),  True),
    StructField("event_type", StringType(),  True),
    StructField("severity",   StringType(),  True),
    StructField("details",    StringType(),  True),
    StructField("method",     StringType(),  True),
    StructField("path",       StringType(),  True),
    StructField("status",     IntegerType(), True),
    StructField("user",       StringType(),  True),
    StructField("port",       IntegerType(), True),
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
# LECTURE KAFKA
# ─────────────────────────────────────────

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", "latest") \
    .load()

events = raw_stream.select(
    from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"),
    col("topic")
).select("e.*", "topic")

events = events.withColumn("event_time", to_timestamp(col("timestamp")))

# ─────────────────────────────────────────
# RÈGLES DE DÉTECTION
# ─────────────────────────────────────────

# Règle 1 — Brute Force SSH
brute_force = events \
    .filter(
        (col("topic") == "auth-logs") &
        (col("event_type") == "FAILED_PASSWORD")
    ) \
    .groupBy(window(col("event_time"), "1 minute"), col("ip")) \
    .agg(count("*").alias("nb_fails")) \
    .filter(col("nb_fails") > 5) \
    .select(
        col("ip"),
        lit("BRUTE_FORCE").alias("alert_type"),
        lit("HIGH").alias("severity"),
        expr("concat('Brute force SSH: ', nb_fails, ' échecs en 1 min depuis ', ip)")
            .alias("details")
    )

# Règle 2 — IP Blacklistée (tous topics)
blacklist_filter = " OR ".join([f"ip = '{ip}'" for ip in BLACKLISTED_IPS])
suspicious_ip = events \
    .filter(blacklist_filter) \
    .select(
        col("ip"),
        lit("SUSPICIOUS_IP").alias("alert_type"),
        lit("CRITICAL").alias("severity"),
        expr("concat('IP blacklistée: ', ip, ' via ', source, ' (', event_type, ')')")
            .alias("details")
    )

# Règle 3 — Accès Web Suspect
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

# Règle 4 — Port Scan
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

# Règle 5 — Heure Inhabituelle
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

# Règle 6 — Firewall Blacklist
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

# Union de toutes les alertes
all_alerts = brute_force \
    .union(suspicious_ip) \
    .union(suspicious_web) \
    .union(port_scan) \
    .union(unusual_time) \
    .union(fw_blacklist)

# ─────────────────────────────────────────
# ÉCRITURE POSTGRESQL + TELEGRAM
# ─────────────────────────────────────────

def write_to_postgres_and_alert(df, epoch_id):
    if df.count() == 0:
        return

    rows = df.collect()
    print(f"\n🔔 Epoch {epoch_id} — {len(rows)} alertes détectées")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        for row in rows:
            # 1. Écrire dans PostgreSQL
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

            # 2. Telegram pour HIGH et CRITICAL uniquement
            if row["severity"] in ["HIGH", "CRITICAL"]:
                send_telegram_alert(
                    row["alert_type"],
                    row["ip"],
                    row["severity"],
                    row["details"]
                )

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erreur PostgreSQL: {e}")

# ─────────────────────────────────────────
# LANCEMENT
# ─────────────────────────────────────────

query = all_alerts.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_postgres_and_alert) \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Spark Streaming lancé — en attente d'événements...")
print(f"   Topics : {TOPICS}")
print(f"   Telegram : activé pour HIGH + CRITICAL")

query.awaitTermination()
