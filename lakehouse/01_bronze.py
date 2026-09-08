"""
01_bronze.py - GempaRadar Lakehouse
Bronze Layer: Ingest data dari HDFS/lokal ke format Parquet (pengganti Delta Lake)
Menambahkan metadata: _ingested_at dan _source

CATATAN KOMPATIBILITAS:
  PySpark 4.1.1 belum didukung oleh delta-spark versi manapun (Delta Lake
  tertinggi hanya support Spark 3.5.x). Oleh karena itu script ini menggunakan
  format Parquet columnar yang secara konsep identik dengan Bronze Delta Layer.
  Time Travel disimulasikan dengan versioning folder manual (versi_0, versi_1, dst).
"""

import os
import subprocess
import json
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# ── Konfigurasi Path ──────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
LOCAL_DATA_DIR  = os.path.join(BASE_DIR, "..", "spark", "data")
BRONZE_DIR      = os.path.join(BASE_DIR, "lakehouse_data", "bronze")

API_LOCAL       = os.path.join(LOCAL_DATA_DIR, "api")
RSS_LOCAL       = os.path.join(LOCAL_DATA_DIR, "rss")

BRONZE_API      = os.path.join(BRONZE_DIR, "gempa_api")
BRONZE_RSS      = os.path.join(BRONZE_DIR, "gempa_rss")

# ── SparkSession (tanpa Delta Lake — kompatibel dengan PySpark 4.x) ───────────
print("=== Inisialisasi SparkSession ===")
spark = (
    SparkSession.builder
    .appName("Bronze-GempaRadar")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    # Naikkan log driver supaya warning tidak mengganggu output
    .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache=WARN")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"✓ SparkSession siap — Spark {spark.version}")
print("  Format penyimpanan: Parquet (columnar, setara Bronze Delta Layer)")


# ── Helper: simpan metadata versi ─────────────────────────────────────────────
def _save_version_meta(path: str, operation: str, row_count: int):
    """
    Simulasi Delta Lake versioning: simpan metadata setiap operasi tulis
    ke file JSON di dalam folder _version_log/.
    Ini yang memungkinkan 'Time Travel' manual di 02_silver.py.
    """
    meta_dir = os.path.join(path, "_version_log")
    os.makedirs(meta_dir, exist_ok=True)

    # Hitung versi berikutnya
    existing = [f for f in os.listdir(meta_dir) if f.endswith(".json")]
    version  = len(existing)

    meta = {
        "version":   version,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "operation": operation,
        "row_count": row_count,
        "format":    "parquet"
    }
    meta_path = os.path.join(meta_dir, f"{version:05d}.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  [version_log] Versi {version} disimpan → {meta_path}")


def _write_parquet(df, path: str, operation: str = "WRITE"):
    """
    Tulis DataFrame ke Parquet dengan mode overwrite,
    lalu simpan snapshot versi untuk Time Travel.
    """
    row_count = df.count()

    # Simpan snapshot versi sebelum overwrite (untuk Time Travel)
    snapshot_dir = os.path.join(os.path.dirname(path),
                                f"_snapshot_{os.path.basename(path)}")
    if os.path.exists(path):
        # Hitung versi snapshot yang sudah ada
        existing_snaps = [d for d in os.listdir(snapshot_dir)
                          if d.startswith("v")] if os.path.exists(snapshot_dir) else []
        snap_ver = len(existing_snaps)
        snap_path = os.path.join(snapshot_dir, f"v{snap_ver:03d}")
        shutil.copytree(path, snap_path)
        print(f"  [snapshot] Versi lama disimpan → {snap_path}")

    df.write.mode("overwrite").parquet(path)
    _save_version_meta(path, operation, row_count)
    return row_count


# ── Sync dari HDFS (opsional) ─────────────────────────────────────────────────
def sync_from_hdfs():
    """Sync data terbaru dari HDFS ke lokal (jika HDFS aktif)."""
    print("\n=== Sync data dari HDFS ===")
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/data/gempa/api/"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("✓ HDFS aktif — sync data terbaru...")
            os.makedirs(API_LOCAL, exist_ok=True)
            os.makedirs(RSS_LOCAL, exist_ok=True)
            subprocess.run(
                ["docker", "exec", "namenode", "hdfs", "dfs",
                 "-get", "-f", "/data/gempa/api/", "/tmp/gempa_api/"],
                capture_output=True
            )
            subprocess.run(
                ["docker", "cp", "namenode:/tmp/gempa_api/.", API_LOCAL],
                capture_output=True
            )
            subprocess.run(
                ["docker", "exec", "namenode", "hdfs", "dfs",
                 "-get", "-f", "/data/gempa/rss/", "/tmp/gempa_rss/"],
                capture_output=True
            )
            subprocess.run(
                ["docker", "cp", "namenode:/tmp/gempa_rss/.", RSS_LOCAL],
                capture_output=True
            )
            print("✓ Sync dari HDFS selesai")
        else:
            print("⚠ HDFS tidak aktif — menggunakan data lokal yang sudah ada")
    except Exception as e:
        print(f"⚠ Tidak bisa connect ke HDFS: {e}")
        print("  Menggunakan data lokal yang sudah ada")


# ── Ingest ke Bronze Layer ────────────────────────────────────────────────────
def ingest_bronze():
    """Ingest data dari lokal ke Bronze Layer (Parquet)."""
    os.makedirs(BRONZE_DIR, exist_ok=True)

    # ── Bronze API ─────────────────────────────────────────────────────────────
    print("\n=== Bronze Layer: Ingest API Data ===")
    if not os.path.isdir(API_LOCAL):
        print(f"✗ Folder API tidak ditemukan: {API_LOCAL}")
        print("  Pastikan producer sudah jalan dan data tersimpan di spark/data/api/")
        return

    api_files = [f for f in os.listdir(API_LOCAL) if f.endswith(".json")]
    print(f"  File ditemukan: {len(api_files)} file JSON")

    if api_files:
        api_df = (
            spark.read
            .option("multiLine", True)
            .json(API_LOCAL)
        )

        bronze_api_df = (
            api_df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source", lit("api"))
        )

        print(f"  Total record API: {bronze_api_df.count()}")
        print("  Schema Bronze API:")
        bronze_api_df.printSchema()

        count = _write_parquet(bronze_api_df, BRONZE_API, "INGEST_API")
        print(f"✓ Bronze API tersimpan ({count} record): {BRONZE_API}")
    else:
        print("✗ Tidak ada file JSON di folder API")

    # ── Bronze RSS ─────────────────────────────────────────────────────────────
    print("\n=== Bronze Layer: Ingest RSS Data ===")
    if not os.path.isdir(RSS_LOCAL):
        print(f"✗ Folder RSS tidak ditemukan: {RSS_LOCAL}")
        return

    rss_files = [f for f in os.listdir(RSS_LOCAL) if f.endswith(".json")]
    print(f"  File ditemukan: {len(rss_files)} file JSON")

    if rss_files:
        rss_df = (
            spark.read
            .option("multiLine", True)
            .json(RSS_LOCAL)
        )

        bronze_rss_df = (
            rss_df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source", lit("rss"))
        )

        count = _write_parquet(bronze_rss_df, BRONZE_RSS, "INGEST_RSS")
        print(f"✓ Bronze RSS tersimpan ({count} record): {BRONZE_RSS}")
    else:
        print("✗ Tidak ada file JSON di folder RSS")

    # ── Verifikasi ─────────────────────────────────────────────────────────────
    print("\n=== Verifikasi Bronze Layer ===")
    if os.path.exists(BRONZE_API):
        print("Sample data Bronze API:")
        spark.read.parquet(BRONZE_API).show(5, truncate=50)

    if os.path.exists(BRONZE_RSS):
        print("Sample data Bronze RSS:")
        spark.read.parquet(BRONZE_RSS).show(5, truncate=50)

    print("\n=== Bronze Layer Selesai! ===")


if __name__ == "__main__":
    sync_from_hdfs()
    ingest_bronze()
    spark.stop()
    print("✓ SparkSession ditutup")
