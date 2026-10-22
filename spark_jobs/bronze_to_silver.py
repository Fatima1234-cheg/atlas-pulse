"""Transformation Bronze → Silver avec PySpark.

Lit les Parquet bruts de data/bronze/, applique les règles de qualité,
écrit des données propres et typées dans data/silver/, et logue chaque
exécution dans data/silver/quality_checks/.
"""
from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession, functions as F

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
QUALITY_DIR = "data/silver/quality_checks"


def create_spark_session() -> SparkSession:
    """Crée la session Spark en mode local (pas besoin de cluster)."""
    return (
        SparkSession.builder
        .appName("atlas-pulse-bronze-to-silver")
        .master("local[*]")               # utilise tous les cœurs disponibles
        .config("spark.sql.shuffle.partitions", "4")  # adapté à un petit volume
        .getOrCreate()
    )


def read_bronze(spark: SparkSession):
    """Lit tous les Parquet de Bronze, récursivement (toutes les villes)."""
    return spark.read.parquet(BRONZE_DIR)


def apply_quality_rules(df):
    """Applique les règles de qualité et retourne (df_clean, metrics)."""
    total = df.count()
    dup_count = total - df.dropDuplicates(["city", "time"]).count()

    # 1. Déduplication
    df = df.dropDuplicates(["city", "time"])

    # 2. Rejet des températures aberrantes
    df = df.filter(
        F.col("temperature_2m").isNotNull()
        & F.col("temperature_2m").between(-20, 60)
    )

    # 3. Rejet des précipitations négatives
    df = df.filter(
        F.col("precipitation").isNotNull()
        & (F.col("precipitation") >= 0)
    )

    clean_count = df.count()
    rejected = total - dup_count - clean_count

    # 4. Ajout d'indicateurs métier
    df = (
        df
        .withColumn("is_heatwave_candidate", F.col("temperature_2m") >= 40)
        .withColumn("is_heavy_rain", F.col("precipitation") >= 10)
    )

    metrics = {
        "rows_received": total,
        "duplicates_removed": dup_count,
        "rows_rejected": rejected,
        "rows_clean": clean_count,
    }
    return df, metrics


def write_silver(df):
    """Écrit la couche Silver partitionnée par ville."""
    (
        df.write
        .mode("overwrite")
        .partitionBy("city")
        .parquet(SILVER_DIR)
    )


def write_quality_log(spark: SparkSession, metrics: dict):
    """Écrit un log de qualité horodaté pour chaque exécution."""
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    log_data = [(run_id, k, v) for k, v in metrics.items()]
    log_df = spark.createDataFrame(log_data, ["run_id", "metric", "value"])
    (
        log_df.write
        .mode("append")
        .partitionBy("run_id")
        .parquet(QUALITY_DIR)
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("→ Lecture de la couche Bronze...")
    bronze = read_bronze(spark)

    print("→ Application des règles de qualité...")
    silver, metrics = apply_quality_rules(bronze)

    print("→ Écriture de la couche Silver...")
    write_silver(silver)

    print("→ Écriture du log de qualité...")
    write_quality_log(spark, metrics)

    print("\n=== Rapport qualité ===")
    for k, v in metrics.items():
        print(f"  {k:22s} : {v}")
    print(f"\nSilver écrit dans : {SILVER_DIR}")
    print(f"Logs qualité dans : {QUALITY_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()