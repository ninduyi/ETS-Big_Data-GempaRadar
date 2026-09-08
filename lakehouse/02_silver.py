"""
02_silver.py - GempaRadar Lakehouse
Silver Layer: Cleaning dan transformasi dari Bronze ke Silver (Parquet)
Minimal 4 transformasi terdokumentasi + demonstrasi Time Travel manual

CATATAN KOMPATIBILITAS:
  Menggunakan Parquet sebagai pengganti Delta format karena PySpark 4.1.1
  belum didukung delta-spark. Time Travel disimulasikan dengan folder snapshot
  yang dibuat otomatis oleh helper _write_parquet() di 01_bronze.py.
"""

import os
import json
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Konfigurasi Path ──────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_DIR   = os.path.join(BASE_DIR, "lakehouse_data")

BRONZE_API      = os.path.join(LAKEHOUSE_DIR, "bronze", "gempa_api")
BRONZE_RSS      = os.path.join(LAKEHOUSE_DIR, "bronze", "gempa_rss")

SILVER_API      = os.path.join(LAKEHOUSE_DIR, "silver", "gempa_api")
SILVER_RSS      = os.path.join(LAKEHOUSE_DIR, "silver", "gempa_rss")

# ── SparkSession ──────────────────────────────────────────────────────────────
print("=== Inisialisasi SparkSession ===")
spark = (
    SparkSession.builder
    .appName("Silver-GempaRadar")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"✓ SparkSession siap — Spark {spark.version}")
print("  Format penyimpanan: Parquet (Bronze → Silver cleaning pipeline)")


# ── Helper: snapshot + tulis Parquet ─────────────────────────────────────────
def _save_version_meta(path: str, operation: str, row_count: int, extra: dict = None):
    meta_dir = os.path.join(path, "_version_log")
    os.makedirs(meta_dir, exist_ok=True)
    existing = [f for f in os.listdir(meta_dir) if f.endswith(".json")]
    version  = len(existing)
    meta = {
        "version":   version,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "operation": operation,
        "row_count": row_count,
        "format":    "parquet"
    }
    if extra:
        meta.update(extra)
    meta_path = os.path.join(meta_dir, f"{version:05d}.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    return version


def _write_parquet(df, path: str, operation: str = "WRITE", extra_meta: dict = None):
    """Tulis ke Parquet dan simpan snapshot untuk Time Travel."""
    row_count = df.count()
    snap_base = os.path.join(os.path.dirname(path),
                             f"_snapshot_{os.path.basename(path)}")

    if os.path.exists(path):
        existing_snaps = [d for d in os.listdir(snap_base)
                          if d.startswith("v")] if os.path.exists(snap_base) else []
        snap_path = os.path.join(snap_base, f"v{len(existing_snaps):03d}")
        shutil.copytree(path, snap_path)
        print(f"  [snapshot] Snapshot v{len(existing_snaps):03d} disimpan → {snap_path}")

    df.write.mode("overwrite").parquet(path)
    ver = _save_version_meta(path, operation, row_count, extra_meta)
    print(f"  [version_log] Versi {ver} → {operation} ({row_count} record)")
    return row_count


# ── Cleaning API ──────────────────────────────────────────────────────────────
def clean_api():
    """Cleaning Bronze API → Silver API dengan 4 transformasi terdokumentasi."""

    print("\n=== Silver Layer: Cleaning API Data ===")
    if not os.path.exists(BRONZE_API):
        print(f"✗ Bronze API tidak ditemukan: {BRONZE_API}")
        print("  Jalankan 01_bronze.py terlebih dahulu.")
        return

    bronze_df     = spark.read.parquet(BRONZE_API)
    total_before  = bronze_df.count()
    print(f"  Total record Bronze API: {total_before}")

    # ── Transformasi 1: Hapus Duplikat ────────────────────────────────────────
    # Alasan: Producer bisa mengirim event yang sama ketika restart.
    # Duplikat membuat hitungan frekuensi gempa per wilayah tidak akurat —
    # satu kejadian gempa bisa terhitung dua kali atau lebih.
    df_dedup    = bronze_df.dropDuplicates(["event_id"])
    after_dedup = df_dedup.count()
    print(f"\n  [T1] Hapus Duplikat berdasarkan event_id:")
    print(f"       Sebelum: {total_before} | Sesudah: {after_dedup} "
          f"| Dihapus: {total_before - after_dedup}")

    # ── Transformasi 2: Filter magnitude tidak valid ───────────────────────────
    # Alasan: Magnitude null atau ≤ 0 tidak bermakna secara seismologi.
    # Data ini akan merusak perhitungan AVG, MAX, dan Risk Score di Gold layer.
    df_valid_mag = df_dedup.filter(
        F.col("magnitude").isNotNull() & (F.col("magnitude") > 0)
    )
    after_mag = df_valid_mag.count()
    print(f"\n  [T2] Filter magnitude null/negatif:")
    print(f"       Sebelum: {after_dedup} | Sesudah: {after_mag} "
          f"| Dihapus: {after_dedup - after_mag}")

    # ── Transformasi 3: Cast tipe data + parse timestamp ──────────────────────
    # Alasan: Semua kolom numerik tersimpan sebagai String di JSON mentah.
    # - magnitude, depth_km, latitude, longitude harus DoubleType agar bisa
    #   digunakan dalam kalkulasi statistik dan Window Functions.
    # - timestamp harus TimestampType agar bisa digunakan untuk analisis
    #   temporal (per jam, per hari, lag comparison) di Gold layer.
    df_typed = (
        df_valid_mag
        .withColumn("magnitude",    F.col("magnitude").cast(DoubleType()))
        .withColumn("depth_km",     F.col("depth_km").cast(DoubleType()))
        .withColumn("latitude",     F.col("latitude").cast(DoubleType()))
        .withColumn("longitude",    F.col("longitude").cast(DoubleType()))
        .withColumn("timestamp_dt", F.to_timestamp(F.col("timestamp")))
        .withColumn("jam",          F.hour(F.col("timestamp_dt")))
        .withColumn("tanggal",      F.to_date(F.col("timestamp_dt")))
    )
    print(f"\n  [T3] Cast tipe data + ekstrak jam dan tanggal dari timestamp:")
    print(f"       Kolom baru: timestamp_dt (TimestampType), jam (Int), tanggal (Date)")
    print(f"       Kolom di-cast: magnitude, depth_km, latitude, longitude → DoubleType")

    # ── Transformasi 4: Filter depth tidak valid ──────────────────────────────
    # Alasan: Kedalaman gempa tidak bisa null atau negatif.
    # Nilai depth_km digunakan untuk menentukan kategori "Dangkal/Menengah/Dalam"
    # yang menjadi komponen Risk Score di Gold layer.
    df_valid_depth = df_typed.filter(
        F.col("depth_km").isNotNull() & (F.col("depth_km") >= 0)
    )
    after_depth = df_valid_depth.count()
    print(f"\n  [T4] Filter depth null/negatif:")
    print(f"       Sebelum: {after_mag} | Sesudah: {after_depth} "
          f"| Dihapus: {after_mag - after_depth}")

    # ── Tambah kolom kategori (derived columns) ───────────────────────────────
    silver_api_df = (
        df_valid_depth
        .withColumn(
            "kategori_mag",
            F.when(F.col("magnitude") < 3.0, "Mikro")
             .when((F.col("magnitude") >= 3.0) & (F.col("magnitude") < 4.0), "Minor")
             .when((F.col("magnitude") >= 4.0) & (F.col("magnitude") < 5.0), "Sedang")
             .when(F.col("magnitude") >= 5.0, "Kuat")
             .otherwise("Unknown")
        )
        .withColumn(
            "kategori_depth",
            F.when(F.col("depth_km") < 70, "Dangkal")
             .when((F.col("depth_km") >= 70) & (F.col("depth_km") < 300), "Menengah")
             .otherwise("Dalam")
        )
    )

    os.makedirs(os.path.dirname(SILVER_API), exist_ok=True)
    total_after = _write_parquet(
        silver_api_df, SILVER_API, "CLEAN_API",
        extra_meta={"dropped_rows": total_before - after_depth}
    )

    print(f"\n  Ringkasan cleaning API:")
    print(f"  Bronze: {total_before} record → Silver: {total_after} record "
          f"({total_before - total_after} dihapus, "
          f"{round((total_before - total_after) / max(total_before, 1) * 100, 1)}%)")
    print(f"✓ Silver API tersimpan: {SILVER_API}")

    print("\n  Sample Silver API:")
    silver_api_df.select(
        "event_id", "magnitude", "kategori_mag",
        "depth_km", "kategori_depth", "wilayah", "jam", "tanggal"
    ).show(5, truncate=40)


# ── Cleaning RSS ──────────────────────────────────────────────────────────────
def clean_rss():
    """Cleaning Bronze RSS → Silver RSS."""

    print("\n=== Silver Layer: Cleaning RSS Data ===")
    if not os.path.exists(BRONZE_RSS):
        print(f"✗ Bronze RSS tidak ditemukan: {BRONZE_RSS}")
        print("  Jalankan 01_bronze.py terlebih dahulu.")
        return

    bronze_rss   = spark.read.parquet(BRONZE_RSS)
    total_before = bronze_rss.count()
    print(f"  Total record Bronze RSS: {total_before}")

    # T1: Hapus duplikat berdasarkan article_id
    df_dedup    = bronze_rss.dropDuplicates(["article_id"])
    after_dedup = df_dedup.count()
    print(f"\n  [T1] Hapus duplikat artikel (article_id): {total_before} → {after_dedup}")

    # T2: Filter artikel tanpa judul atau judul terlalu pendek
    df_valid = df_dedup.filter(
        F.col("title").isNotNull() & (F.length(F.col("title")) > 5)
    )
    after_valid = df_valid.count()
    print(f"  [T2] Filter artikel tanpa judul / judul < 5 karakter: "
          f"{after_dedup} → {after_valid}")

    # T3: Parse tanggal publikasi → timestamp
    df_typed = (
        df_valid
        .withColumn("published_dt",   F.to_timestamp(F.col("published")))
        .withColumn("jam_publikasi",   F.hour(F.col("published_dt")))
        .withColumn("tanggal_pub",     F.to_date(F.col("published_dt")))
    )
    print(f"  [T3] Parse timestamp published → published_dt, jam_publikasi, tanggal_pub")

    os.makedirs(os.path.dirname(SILVER_RSS), exist_ok=True)
    total_after = _write_parquet(df_typed, SILVER_RSS, "CLEAN_RSS")
    print(f"\n  Ringkasan cleaning RSS: Bronze {total_before} → Silver {total_after}")
    print(f"✓ Silver RSS tersimpan: {SILVER_RSS}")


# ── Demonstrasi Time Travel ───────────────────────────────────────────────────
def demo_time_travel():
    """
    Demonstrasi Time Travel (simulasi Delta Lake versioning).

    Cara kerja:
      1. Baca Silver API versi sekarang (setelah clean_api).
      2. Lakukan simulasi 'update' — ubah semua 'Unknown' → 'Mikro'.
      3. Simpan sebagai versi baru (snapshot versi lama otomatis tersimpan).
      4. Baca kembali versi lama dari folder snapshot.
      5. Bandingkan distribusi kategori_mag sebelum vs sesudah update.
    """

    print("\n=== Demonstrasi Time Travel (Simulasi Delta Lake Versioning) ===")

    if not os.path.exists(SILVER_API):
        print("✗ Silver API belum ada, jalankan clean_api() terlebih dahulu.")
        return

    # ── Baca versi saat ini ────────────────────────────────────────────────────
    current_df = spark.read.parquet(SILVER_API)
    ver_log_dir = os.path.join(SILVER_API, "_version_log")
    versions    = sorted(os.listdir(ver_log_dir)) if os.path.exists(ver_log_dir) else []

    print(f"\n--- History Tabel Silver API ({len(versions)} versi) ---")
    print(f"{'Ver':>4}  {'Waktu':25}  {'Operasi':20}  {'Baris':>8}")
    print("-" * 65)
    for vf in versions:
        with open(os.path.join(ver_log_dir, vf)) as f:
            meta = json.load(f)
        print(f"{meta['version']:>4}  {meta['timestamp']:25}  "
              f"{meta['operation']:20}  {meta['row_count']:>8}")

    print("\n--- Distribusi kategori_mag SEBELUM update ---")
    current_df.groupBy("kategori_mag").count().orderBy("count", ascending=False).show()

    # ── Simulasi update: 'Unknown' → 'Mikro' ──────────────────────────────────
    print("--- Melakukan update: set kategori_mag 'Unknown' → 'Mikro' ---")
    unknown_count = current_df.filter(F.col("kategori_mag") == "Unknown").count()
    print(f"  Record yang akan diubah: {unknown_count}")

    updated_df = current_df.withColumn(
        "kategori_mag",
        F.when(F.col("kategori_mag") == "Unknown", F.lit("Mikro"))
         .otherwise(F.col("kategori_mag"))
    )

    # Simpan versi baru (snapshot otomatis menyimpan versi lama)
    updated_rows = updated_df.collect()
    updated_df2 = spark.createDataFrame(updated_rows, schema=updated_df.schema)
    _write_parquet(updated_df2, SILVER_API, "UPDATE_UNKNOWN_TO_MIKRO")

    print("\n--- Distribusi kategori_mag SESUDAH update ---")
    spark.read.parquet(SILVER_API) \
        .groupBy("kategori_mag").count() \
        .orderBy("count", ascending=False).show()

    # ── Baca versi lama dari snapshot ─────────────────────────────────────────
    snap_base = os.path.join(
        os.path.dirname(SILVER_API),
        f"_snapshot_{os.path.basename(SILVER_API)}"
    )
    if os.path.exists(snap_base):
        snaps = sorted(os.listdir(snap_base))
        if snaps:
            # Baca snapshot paling awal (versi 0 = hasil clean_api)
            snap_v0 = os.path.join(snap_base, snaps[0])
            print(f"--- Distribusi kategori_mag VERSI LAMA ({snaps[0]}) ---")
            spark.read.parquet(snap_v0) \
                .groupBy("kategori_mag").count() \
                .orderBy("count", ascending=False).show()

    print("✓ Time Travel berhasil!")
    print("  Snapshot versi lama tersimpan di:", snap_base)
    print("  Konsep ini identik dengan Delta Lake 'versionAsOf' —")
    print("  perbedaannya hanya pada mekanisme internal (Delta pakai transaction log,")
    print("  kita pakai folder snapshot karena keterbatasan kompatibilitas PySpark 4.x)")


if __name__ == "__main__":
    clean_api()
    clean_rss()
    demo_time_travel()
    spark.stop()
    print("✓ SparkSession ditutup")
