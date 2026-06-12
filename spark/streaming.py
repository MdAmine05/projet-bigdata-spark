import time

# Attendre que Kafka soit prêt
print("⏳ Attente de Kafka...")
time.sleep(30)
print("✅ Démarrage de Spark...")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, 
    current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StringType, 
    DoubleType, IntegerType
)
import psycopg2
import json

# ─── Configuration PostgreSQL ───────────────────────
DB_CONFIG = {
    "host": "postgres",
    "database": "securitydb",
    "user": "admin",
    "password": "secret"
}

# IPs blacklistées
BLACKLIST_IPS = [
    "185.220.101.1",
    "45.33.32.156",
    "192.168.1.666",
    "10.0.0.666",
    "222.186.30.111"
]

# Ports normaux autorisés
NORMAL_PORTS = [22, 80, 443, 8080]

# ─── Spark Session ──────────────────────────────────
spark = SparkSession.builder \
    .appName("SecurityStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─── Schema des messages JSON ───────────────────────
schema = StructType() \
    .add("timestamp", DoubleType()) \
    .add("ip", StringType()) \
    .add("user", StringType()) \
    .add("action", StringType()) \
    .add("port", IntegerType()) \
    .add("raw", StringType())

# ─── Lecture depuis Kafka ───────────────────────────
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "security-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parser le JSON
events = raw_stream \
    .select(from_json(
        col("value").cast("string"), schema
    ).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", 
        (col("timestamp")).cast("timestamp"))

# ─── Règle 1 : Brute Force ──────────────────────────
# > 5 echecs login depuis la meme IP en 1 minute
brute_force = events \
    .filter(col("action") == "login_fail") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("ip")
    ) \
    .agg(count("*").alias("attempts")) \
    .filter(col("attempts") > 5) \
    .select(
        col("window.start").alias("timestamp"),
        col("ip"),
        lit("BRUTE_FORCE").alias("alert_type"),
        col("attempts").cast("string").alias("details"),
        lit("HIGH").alias("severity")
    )

# ─── Règle 2 : IP Suspecte ──────────────────────────
suspicious_ip = events \
    .filter(col("ip").isin(BLACKLIST_IPS)) \
    .select(
        col("event_time").alias("timestamp"),
        col("ip"),
        lit("SUSPICIOUS_IP").alias("alert_type"),
        col("raw").alias("details"),
        lit("CRITICAL").alias("severity")
    )

# ─── Règle 3 : Port Inhabituel ──────────────────────
unusual_port = events \
    .filter(
        col("port").isNotNull() & 
        ~col("port").isin(NORMAL_PORTS)
    ) \
    .select(
        col("event_time").alias("timestamp"),
        col("ip"),
        lit("UNUSUAL_PORT").alias("alert_type"),
        col("port").cast("string").alias("details"),
        lit("MEDIUM").alias("severity")
    )

# ─── Règle 4 : Heure Inhabituelle ───────────────────
# Connexions entre 00h et 05h
from pyspark.sql.functions import hour
unusual_time = events \
    .filter(
        (hour(col("event_time")) >= 0) & 
        (hour(col("event_time")) < 5)
    ) \
    .select(
        col("event_time").alias("timestamp"),
        col("ip"),
        lit("UNUSUAL_TIME").alias("alert_type"),
        col("raw").alias("details"),
        lit("LOW").alias("severity")
    )

# ─── Ecriture vers PostgreSQL ───────────────────────
def write_to_postgres(df, epoch_id, table="alerts"):
    if df.count() == 0:
        return
    rows = df.collect()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO alerts 
            (timestamp, ip, alert_type, details, severity)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row["timestamp"],
            row["ip"],
            row["alert_type"],
            row["details"],
            row["severity"]
        ))
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {len(rows)} alertes écrites dans PostgreSQL")

# ─── Ecriture vers Parquet ──────────────────────────
def write_to_parquet(df, epoch_id):
    if df.count() == 0:
        return
    df.write.mode("append").parquet("/tmp/security_history")
    print(f"✅ Historique sauvegardé en Parquet")

# ─── Lancer les queries ─────────────────────────────
q1 = brute_force.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid)) \
    .trigger(processingTime="10 seconds") \
    .start()

q2 = suspicious_ip.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid)) \
    .trigger(processingTime="10 seconds") \
    .start()

q3 = unusual_port.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid)) \
    .trigger(processingTime="10 seconds") \
    .start()

q4 = unusual_time.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, eid: write_to_postgres(df, eid)) \
    .trigger(processingTime="10 seconds") \
    .start()

# Historique complet en Parquet
q5 = events.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_parquet) \
    .trigger(processingTime="30 seconds") \
    .start()

print("🔥 Spark Streaming démarré — détection en cours...")
spark.streams.awaitAnyTermination()
