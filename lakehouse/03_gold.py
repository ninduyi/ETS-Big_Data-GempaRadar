"""
03_gold.py - GempaRadar Lakehouse
Gold Layer: Agregasi dan analisis lanjutan dari Silver (Parquet)

Tabel Gold:
  Gold 1 — gempa_mag_dist          ✅ Repro ETS  Distribusi kategori magnitudo
  Gold 2 — gempa_region_rank       ✅ Repro ETS  Wilayah paling aktif gempa
  Gold 3 — gempa_risk_score        🆕 Enhanced   Risk Score per wilayah
  Gold 4 — gempa_significant_alerts🆕 Enhanced   Cross-source join API + RSS
  Gold 5 — gempa_hourly_activity   🆕 Enhanced   Temporal analysis per jam

CATATAN KOMPATIBILITAS:
  Menggunakan Parquet (bukan Delta format) karena PySpark 4.1.1 belum
  didukung delta-spark. Semua logika agregasi, Window Function, dan
  cross-source join tetap identik dengan spesifikasi tugas.
"""

import os
import json
import shutil
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ── Konfigurasi Path ──────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
LAKEHOUSE_DIR   = os.path.join(BASE_DIR, "lakehouse_data")

SILVER_API      = os.path.join(LAKEHOUSE_DIR, "silver", "gempa_api")
SILVER_RSS      = os.path.join(LAKEHOUSE_DIR, "silver", "gempa_rss")
GOLD_DIR        = os.path.join(LAKEHOUSE_DIR, "gold")

# ── SparkSession ──────────────────────────────────────────────────────────────
print("=== Inisialisasi SparkSession ===")
spark = (
    SparkSession.builder
    .appName("Gold-GempaRadar")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"✓ SparkSession siap — Spark {spark.version}")


# ── Helper ────────────────────────────────────────────────────────────────────
def _write_gold(df, name: str):
    """Tulis tabel Gold ke Parquet dan simpan metadata."""
    path = os.path.join(GOLD_DIR, name)
    os.makedirs(GOLD_DIR, exist_ok=True)
    df.write.mode("overwrite").parquet(path)
    count = spark.read.parquet(path).count()

    # Simpan metadata Gold
    meta_path = os.path.join(GOLD_DIR, f"_{name}_meta.json")
    with open(meta_path, "w") as f:
        json.dump({
            "table":     name,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "row_count": count,
            "format":    "parquet"
        }, f, indent=2)
    print(f"✓ Gold '{name}' tersimpan ({count} record): {path}")
    return count


# ── Cek Silver tersedia ───────────────────────────────────────────────────────
if not os.path.exists(SILVER_API):
    print(f"✗ Silver API tidak ditemukan: {SILVER_API}")
    print("  Jalankan 01_bronze.py lalu 02_silver.py terlebih dahulu.")
    spark.stop()
    exit(1)

if not os.path.exists(SILVER_RSS):
    print(f"⚠ Silver RSS tidak ditemukan — Gold 4 (cross-source join) akan dilewati")

silver_api = spark.read.parquet(SILVER_API)
total_api  = silver_api.count()
print(f"✓ Silver API dimuat: {total_api} record")

silver_api.createOrReplaceTempView("gempa_silver")

silver_rss = None
total_rss  = 0
if os.path.exists(SILVER_RSS):
    silver_rss = spark.read.parquet(SILVER_RSS)
    total_rss  = silver_rss.count()
    silver_rss.createOrReplaceTempView("berita_silver")
    print(f"✓ Silver RSS dimuat: {total_rss} record")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD 1 — Distribusi Magnitudo (Reproduksi ETS)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== GOLD 1: Distribusi Magnitudo (Repro ETS) ===")

gold_mag = spark.sql(f"""
    SELECT
        kategori_mag,
        COUNT(*)                               AS jumlah,
        ROUND(AVG(magnitude), 2)               AS rata_rata_mag,
        ROUND(MAX(magnitude), 2)               AS mag_tertinggi,
        ROUND(MIN(magnitude), 2)               AS mag_terendah,
        ROUND(COUNT(*) / {total_api} * 100, 1) AS persentase
    FROM gempa_silver
    GROUP BY kategori_mag
    ORDER BY jumlah DESC
""")

print("Hasil Gold 1 — Distribusi Magnitudo:")
gold_mag.show()
_write_gold(gold_mag, "gempa_mag_dist")

print("""
  [Perbandingan vs ETS]
  ETS    : Membaca JSON mentah dari HDFS, tipe data belum tentu benar,
           ada kemungkinan duplikat, magnitude masih String.
  Gold 1 : Membaca Silver yang sudah di-cast ke DoubleType dan bebas duplikat.
           Agregasi AVG/MAX/MIN akurat karena data sudah bersih.
""")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD 2 — Wilayah Paling Aktif (Reproduksi ETS)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== GOLD 2: Wilayah Paling Aktif (Repro ETS) ===")

gold_wilayah = spark.sql("""
    SELECT
        wilayah,
        COUNT(*)                                           AS jumlah_gempa,
        ROUND(AVG(magnitude), 2)                           AS rata_rata_mag,
        ROUND(MAX(magnitude), 2)                           AS mag_tertinggi,
        SUM(CASE WHEN magnitude >= 5.0 THEN 1 ELSE 0 END) AS gempa_kuat,
        SUM(CASE WHEN kategori_depth = 'Dangkal' THEN 1 ELSE 0 END)
                                                           AS gempa_dangkal
    FROM gempa_silver
    WHERE wilayah IS NOT NULL AND wilayah != 'Unknown'
    GROUP BY wilayah
    ORDER BY jumlah_gempa DESC
    LIMIT 10
""")

print("Hasil Gold 2 — Top 10 Wilayah Paling Aktif:")
gold_wilayah.show(truncate=50)
_write_gold(gold_wilayah, "gempa_region_rank")

print("""
  [Perbandingan vs ETS]
  ETS    : Tidak ada kolom gempa_dangkal — depth belum diekstrak saat analisis.
  Gold 2 : Kolom gempa_dangkal tersedia karena Silver sudah punya kategori_depth.
           Informasi lebih lengkap untuk BPBD menentukan prioritas wilayah.
""")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD 3 — Risk Score per Wilayah (Enhanced — TIDAK ADA di ETS)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== GOLD 3: Risk Score per Wilayah (Enhanced) ===")
print("Formula: risk_score = frekuensi × avg_mag × (1 + pct_dangkal)")
print("Insight : Wilayah dengan gempa sering + magnitude tinggi + dangkal = paling berbahaya")

gold_risk = spark.sql("""
    SELECT
        wilayah,
        COUNT(*)                                       AS frekuensi,
        ROUND(AVG(magnitude), 2)                       AS avg_mag,
        ROUND(
            COUNT(CASE WHEN depth_km < 70 THEN 1 END)
            / COUNT(*), 2
        )                                              AS pct_dangkal,
        ROUND(
            COUNT(*) * AVG(magnitude)
            * (1 + COUNT(CASE WHEN depth_km < 70 THEN 1 END) / COUNT(*)),
            2
        )                                              AS risk_score,
        MAX(magnitude)                                 AS max_mag_pernah_terjadi
    FROM gempa_silver
    WHERE wilayah IS NOT NULL AND wilayah != 'Unknown'
    GROUP BY wilayah
    HAVING COUNT(*) >= 1
    ORDER BY risk_score DESC
    LIMIT 10
""")

print("Hasil Gold 3 — Top 10 Wilayah Berisiko Tinggi:")
gold_risk.show(truncate=40)
_write_gold(gold_risk, "gempa_risk_score")

print("""
  [Keunggulan vs ETS]
  ETS    : Hanya ranking berdasarkan frekuensi saja.
  Gold 3 : Risk Score menggabungkan 3 faktor sekaligus:
           - Frekuensi gempa (seberapa sering)
           - Rata-rata magnitude (seberapa kuat)
           - Persentase gempa dangkal (seberapa berbahaya bagi penduduk)
  Analisis ini tidak bisa dibuat di ETS karena depth belum di-cast dan
  tidak ada derived metric seperti risk_score.
""")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD 4 — Significant Alerts: Cross-source Join API + RSS (Enhanced)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== GOLD 4: Significant Alerts — Cross-source Join API + RSS (Enhanced) ===")

if silver_rss is not None:
    print("Insight: Gempa M>4.5 yang diikuti berita kebencanaan dalam 2 jam sesudahnya")

    # Gempa signifikan M > 4.5
    gempa_sig = silver_api.filter(F.col("magnitude") > 4.5).select(
        "event_id", "magnitude", "wilayah", "place",
        "timestamp_dt", "depth_km", "kategori_mag"
    )

    # Berita terkait gempa
    berita_gempa = silver_rss.filter(
        F.lower(F.col("title")).rlike("gempa|earthquake|tsunami|seismik|magnitude")
    ).select("article_id", "title", "source", "published_dt")

    # Cross-source join dengan filter waktu 2 jam
    gold_alerts = (
        gempa_sig.alias("g")
        .join(
            berita_gempa.alias("b"),
            (F.col("b.published_dt") >= F.col("g.timestamp_dt")) &
            (F.col("b.published_dt") <= F.col("g.timestamp_dt") +
             F.expr("INTERVAL 2 HOURS")),
            how="left"
        )
        .groupBy(
            "g.event_id", "g.magnitude", "g.wilayah",
            "g.timestamp_dt", "g.kategori_mag", "g.depth_km"
        )
        .agg(
            F.count("b.article_id").alias("jumlah_berita_terkait"),
            F.collect_list("b.title").alias("judul_berita")
        )
        .withColumn(
            "alert_level",
            F.when(
                (F.col("magnitude") >= 6.0) & (F.col("jumlah_berita_terkait") > 0),
                "🔴 KRITIS"
            ).when(
                (F.col("magnitude") >= 5.0) & (F.col("jumlah_berita_terkait") > 0),
                "🟠 TINGGI"
            ).when(F.col("magnitude") >= 4.5, "🟡 WASPADA")
             .otherwise("⚪ NORMAL")
        )
        .orderBy("magnitude", ascending=False)
    )

    print(f"Total gempa signifikan M>4.5: {gempa_sig.count()}")
    print("Hasil Gold 4 — Significant Alerts:")
    gold_alerts.select(
        "event_id", "magnitude", "wilayah",
        "alert_level", "jumlah_berita_terkait", "kategori_mag"
    ).show(10, truncate=40)
    _write_gold(gold_alerts, "gempa_significant_alerts")

    print("""
  [Keunggulan vs ETS]
  ETS    : API dan RSS dianalisis terpisah, tidak ada join antar sumber.
  Gold 4 : Cross-source join antara data gempa (API) + berita (RSS).
           Menghasilkan insight baru: apakah gempa besar diikuti pemberitaan?
           Alert level membantu BPBD memprioritaskan respons kebencanaan.
""")
else:
    print("⚠ Silver RSS tidak ada — Gold 4 dilewati. "
          "Jalankan 01_bronze.py + 02_silver.py dengan data RSS.")


# ══════════════════════════════════════════════════════════════════════════════
# GOLD 5 — Temporal Analysis per Jam dengan Window Function (Enhanced)
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== GOLD 5: Tren Aktivitas per Jam (Window Function — Enhanced) ===")
print("Analisis ini tidak mungkin di ETS karena timestamp belum di-parse ke TimestampType")

gold_temporal = spark.sql("""
    SELECT
        jam,
        COUNT(*)                                           AS jumlah_gempa,
        ROUND(AVG(magnitude), 2)                           AS avg_mag,
        MAX(magnitude)                                     AS max_mag,
        SUM(CASE WHEN magnitude >= 5.0 THEN 1 ELSE 0 END) AS gempa_kuat,
        SUM(CASE WHEN kategori_depth = 'Dangkal' THEN 1 ELSE 0 END)
                                                           AS gempa_dangkal
    FROM gempa_silver
    WHERE jam IS NOT NULL
    GROUP BY jam
    ORDER BY jam
""")

print("Aktivitas gempa per jam (0–23):")
gold_temporal.show(24)
_write_gold(gold_temporal, "gempa_hourly_activity")

# ── Window Function: lag comparison (aktivitas naik/turun antar jam) ──────────
print("\n--- Window Function: Perbandingan Aktivitas Antar Jam ---")
window_spec   = Window.orderBy("jam")
gold_trend_df = gold_temporal \
    .withColumn("prev_jumlah", F.lag("jumlah_gempa", 1).over(window_spec)) \
    .withColumn("delta_gempa",  F.col("jumlah_gempa") - F.col("prev_jumlah")) \
    .withColumn(
        "tren",
        F.when(F.col("delta_gempa") > 0,  "↑ Naik")
         .when(F.col("delta_gempa") < 0,  "↓ Turun")
         .when(F.col("delta_gempa") == 0, "→ Stabil")
         .otherwise("–")
    )

print("Tren aktivitas gempa per jam:")
gold_trend_df.select("jam", "jumlah_gempa", "prev_jumlah",
                     "delta_gempa", "tren").show(24)
_write_gold(gold_trend_df, "gempa_hourly_trend")


# ══════════════════════════════════════════════════════════════════════════════
# Ringkasan semua Gold table
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== Ringkasan Gold Layer ===")
gold_tables = [
    ("gempa_mag_dist",           "✅ Repro ETS", "Distribusi magnitudo"),
    ("gempa_region_rank",        "✅ Repro ETS", "Wilayah paling aktif"),
    ("gempa_risk_score",         "🆕 Enhanced",  "Risk score per wilayah"),
    ("gempa_significant_alerts", "🆕 Enhanced",  "Cross-source join API + RSS"),
    ("gempa_hourly_activity",    "🆕 Enhanced",  "Temporal analysis per jam"),
    ("gempa_hourly_trend",       "🆕 Enhanced",  "Window Function lag comparison"),
]

print(f"  {'Tipe':15} {'Nama Tabel':30} {'Record':>8}  {'Deskripsi'}")
print("  " + "-" * 80)
for name, tipe, desc in gold_tables:
    path = os.path.join(GOLD_DIR, name)
    if os.path.exists(path):
        count = spark.read.parquet(path).count()
        print(f"  {tipe:15} {name:30} {count:>8}  {desc}")
    else:
        print(f"  {tipe:15} {name:30} {'N/A':>8}  {desc} (dilewati)")

print("\n=== Gold Layer Selesai! ===")

spark.stop()
print("✓ SparkSession ditutup")
