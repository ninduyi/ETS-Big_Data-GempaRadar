"""
analysis.py - GempaRadar
[NamaAnggota4]: Spark Analysis Script — berjalan otomatis setiap 10 menit
Membaca dari: spark/data/api/ dan spark/data/rss/
Menyimpan hasil ke: spark/data/hasil/ dan dashboard/data/spark_results.json
"""

import json
import os
import time
import logging
import subprocess
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Konfigurasi ──────────────────────────────────────────────────────────────
RUN_INTERVAL    = 600   # jalankan analisis setiap 10 menit
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
DATA_API        = os.path.join(BASE_DIR, "data", "api")
DATA_RSS        = os.path.join(BASE_DIR, "data", "rss")
DATA_HASIL      = os.path.join(BASE_DIR, "data", "hasil")
DASHBOARD_DIR   = os.path.join(BASE_DIR, "..", "dashboard", "data")

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def sync_from_hdfs():
    """Sync data terbaru dari HDFS ke lokal sebelum analisis."""
    log.info("Sync data dari HDFS...")
    os.makedirs("/tmp/gempa_api", exist_ok=True)
    os.makedirs("/tmp/gempa_rss", exist_ok=True)
    os.makedirs(DATA_API, exist_ok=True)
    os.makedirs(DATA_RSS, exist_ok=True)
    os.makedirs(DATA_HASIL, exist_ok=True)
    os.makedirs(DASHBOARD_DIR, exist_ok=True)

    # Copy dari HDFS ke container lokal lalu ke host
    cmds = [
        ["docker", "exec", "namenode", "hdfs", "dfs", "-get", "-f",
         "/data/gempa/api/", "/tmp/gempa_api/"],
        ["docker", "exec", "namenode", "hdfs", "dfs", "-get", "-f",
         "/data/gempa/rss/", "/tmp/gempa_rss/"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.warning(f"HDFS get warning: {result.stderr.strip()}")

    # Docker cp ke lokal
    subprocess.run(["docker", "cp", "namenode:/tmp/gempa_api/.", DATA_API],
                   capture_output=True)
    subprocess.run(["docker", "cp", "namenode:/tmp/gempa_rss/.", DATA_RSS],
                   capture_output=True)
    log.info("Sync selesai")


def run_analysis(spark: SparkSession):
    """Jalankan 3 analisis Spark dan simpan hasilnya."""

    # Cek apakah ada data
    api_files = [f for f in os.listdir(DATA_API) if f.endswith(".json")]
    if not api_files:
        log.warning("Belum ada data di spark/data/api/ — skip analisis")
        return

    log.info(f"Membaca {len(api_files)} file dari {DATA_API}")

    # Baca data
    df_api = spark.read.option("multiLine", True).json(DATA_API)
    df_rss = spark.read.option("multiLine", True).json(DATA_RSS)

    total = df_api.count()
    log.info(f"Total event API: {total}")
    log.info(f"Total artikel RSS: {df_rss.count()}")

    if total == 0:
        log.warning("Data kosong — skip analisis")
        return

    # ── Analisis 1: Distribusi Magnitudo ──────────────────────────────────────
    log.info("Menjalankan Analisis 1: Distribusi Magnitudo...")
    df_mag = df_api.withColumn(
        "kategori_mag",
        F.when(F.col("magnitude") < 3.0, "Mikro (< 3.0)")
         .when((F.col("magnitude") >= 3.0) & (F.col("magnitude") < 4.0), "Minor (3.0–3.9)")
         .when((F.col("magnitude") >= 4.0) & (F.col("magnitude") < 5.0), "Sedang (4.0–4.9)")
         .when(F.col("magnitude") >= 5.0, "Kuat (≥ 5.0)")
         .otherwise("Tidak Diketahui")
    )

    hasil_mag = (
        df_mag.groupBy("kategori_mag")
        .agg(
            F.count("*").alias("jumlah"),
            F.round(F.avg("magnitude"), 2).alias("rata_rata_mag"),
            F.round(F.max("magnitude"), 2).alias("mag_tertinggi"),
        )
        .withColumn("persentase", F.round(F.col("jumlah") / total * 100, 1))
        .orderBy("jumlah", ascending=False)
    )
    hasil_mag.show(truncate=False)

    # ── Analisis 2: Wilayah Paling Aktif (Spark SQL) ──────────────────────────
    log.info("Menjalankan Analisis 2: Wilayah Paling Aktif...")
    df_mag.createOrReplaceTempView("gempa")

    hasil_wilayah = spark.sql("""
        SELECT
            wilayah,
            COUNT(*) AS jumlah_gempa,
            ROUND(AVG(magnitude), 2) AS rata_rata_mag,
            ROUND(MAX(magnitude), 2) AS mag_tertinggi,
            ROUND(MIN(magnitude), 2) AS mag_terendah,
            SUM(CASE WHEN magnitude >= 5.0 THEN 1 ELSE 0 END) AS gempa_kuat
        FROM gempa
        WHERE wilayah IS NOT NULL AND wilayah != 'Unknown'
        GROUP BY wilayah
        ORDER BY jumlah_gempa DESC
        LIMIT 10
    """)
    hasil_wilayah.show(truncate=60)

    # ── Analisis 3: Distribusi Kedalaman (Spark SQL) ──────────────────────────
    log.info("Menjalankan Analisis 3: Distribusi Kedalaman...")
    hasil_kedalaman = spark.sql("""
        SELECT
            CASE
                WHEN depth_km < 70  THEN 'Dangkal (< 70 km)'
                WHEN depth_km < 300 THEN 'Menengah (70–300 km)'
                ELSE                     'Dalam (> 300 km)'
            END AS kategori_kedalaman,
            COUNT(*) AS jumlah,
            ROUND(AVG(depth_km), 1) AS rata_rata_depth_km,
            ROUND(MIN(depth_km), 1) AS min_depth_km,
            ROUND(MAX(depth_km), 1) AS max_depth_km,
            ROUND(AVG(magnitude), 2) AS rata_rata_mag
        FROM gempa
        WHERE depth_km IS NOT NULL
        GROUP BY kategori_kedalaman
        ORDER BY jumlah DESC
    """)
    hasil_kedalaman.show(truncate=False)

    # ── Simpan hasil ke dashboard/data/spark_results.json ────────────────────
    log.info("Menyimpan hasil ke spark_results.json...")
    mag_data     = hasil_mag.toPandas().to_dict(orient="records")
    wilayah_data = hasil_wilayah.toPandas().to_dict(orient="records")
    depth_data   = hasil_kedalaman.toPandas().to_dict(orient="records")

    spark_results = {
        "generated_at"                   : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_events"                   : total,
        "analisis_1_distribusi_magnitudo": mag_data,
        "analisis_2_wilayah_aktif"       : wilayah_data,
        "analisis_3_distribusi_kedalaman": depth_data,
    }

    output_path = os.path.join(DASHBOARD_DIR, "spark_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(spark_results, f, ensure_ascii=False, indent=2)

    log.info(f"✓ spark_results.json tersimpan: {output_path}")
    log.info(f"  Total event dianalisis: {total}")


def main():
    log.info("=== GempaRadar Spark Analysis — Mode Otomatis ===")
    log.info(f"Analisis akan dijalankan setiap {RUN_INTERVAL // 60} menit")

    # Buat SparkSession sekali saja
    spark = (
        SparkSession.builder
        .appName("GempaRadar-Analysis-Auto")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info(f"✓ SparkSession dibuat — Spark {spark.version}")

    run_count = 0
    while True:
        run_count += 1
        log.info(f"=== Siklus analisis #{run_count} ===")

        try:
            sync_from_hdfs()
            run_analysis(spark)
        except Exception as e:
            log.error(f"Error saat analisis: {e}")

        log.info(f"Menunggu {RUN_INTERVAL // 60} menit untuk siklus berikutnya...")
        time.sleep(RUN_INTERVAL)


if __name__ == "__main__":
    main()
