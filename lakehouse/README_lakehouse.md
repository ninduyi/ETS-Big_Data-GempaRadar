# 🏗️ GempaRadar — Data Lakehouse Upgrade

**Mata Kuliah:** Big Data dan Data Lakehouse  
**Tugas:** Upgrade Pipeline ETS ke Data Lakehouse (Medallion Architecture)  
**Topik:** GempaRadar — Monitoring Gempa Bumi Real-time Indonesia

---

## 📊 Arsitektur: Sebelum vs Sesudah

### SEBELUM (ETS)
```
[USGS API]  → producer_api.py → Kafka (gempa-api) ──┐
[RSS Feed]  → producer_rss.py → Kafka (gempa-rss) ──┤
                                                      └→ consumer_to_hdfs.py
                                                              │
                                                     HDFS /data/gempa/ (JSON mentah)
                                                              │
                                                       spark/analysis.py
                                                    (3 analisis, simpan JSON biasa)
                                                              │
                                                      Flask Dashboard
```

**Masalah di ETS:**
- Data tersimpan sebagai JSON mentah — tidak ada schema enforcement
- Tidak ada versioning — jika data diupdate, versi lama hilang selamanya
- Timestamp masih String — tidak bisa pakai Window Functions untuk analisis temporal
- API dan RSS dianalisis terpisah — tidak ada insight lintas sumber
- Duplikat bisa masuk jika producer restart — hitungan frekuensi gempa tidak akurat

### SESUDAH (Data Lakehouse — Tugas Ini)
```
[USGS API]  → producer_api.py → Kafka (gempa-api) ──┐
[RSS Feed]  → producer_rss.py → Kafka (gempa-rss) ──┤
                                                      └→ consumer_to_hdfs.py
                                                              │
                                                     HDFS /data/gempa/ (JSON mentah)
                                                              │
                                              ┌───────────────▼───────────────┐
                                              │         BRONZE LAYER          │
                                              │   Format: Parquet Columnar    │
                                              │   + metadata _ingested_at     │
                                              │   + metadata _source          │
                                              │   + version log (versioning)  │
                                              └───────────────┬───────────────┘
                                                              │
                                              ┌───────────────▼───────────────┐
                                              │         SILVER LAYER          │
                                              │   Format: Parquet Columnar    │
                                              │   T1: Hapus duplikat          │
                                              │   T2: Filter magnitude invalid│
                                              │   T3: Cast tipe data + timestamp│
                                              │   T4: Filter depth invalid    │
                                              │   + kategori_mag, kategori_depth│
                                              │   + Time Travel (snapshot)    │
                                              └───────────────┬───────────────┘
                                                              │
                                              ┌───────────────▼───────────────┐
                                              │          GOLD LAYER           │
                                              │   Format: Parquet Columnar    │
                                              │   Gold 1: Distribusi Mag      │
                                              │   Gold 2: Wilayah Aktif       │
                                              │   Gold 3: Risk Score 🆕       │
                                              │   Gold 4: Cross-source Join 🆕│
                                              │   Gold 5: Temporal Analysis 🆕│
                                              └───────────────┬───────────────┘
                                                              │
                                                      Flask Dashboard
```

> **Catatan Kompatibilitas:** Script menggunakan format **Parquet** (bukan Delta Lake)
> karena PySpark 4.1.1 belum didukung oleh `delta-spark` versi manapun. Parquet adalah
> format file yang sama digunakan Delta Lake secara internal. Time Travel disimulasikan
> dengan mekanisme folder snapshot + version log JSON.

---

## 🔧 Cara Menjalankan

```bash
cd ~/gemparadar
source venv/bin/activate

# Tidak perlu install delta-spark — script sudah tidak membutuhkannya
# Cukup pastikan PySpark tersedia:
python3 -c "import pyspark; print('OK:', pyspark.__version__)"

# Step 1 — Bronze: ingest data ke Parquet
python3 lakehouse/01_bronze.py

# Step 2 — Silver: cleaning dan transformasi
python3 lakehouse/02_silver.py

# Step 3 — Gold: agregasi dan analisis lanjutan
python3 lakehouse/03_gold.py
```

Jika HDFS tidak aktif, script otomatis menggunakan data lokal di `spark/data/`.

---

## 🧹 Penjelasan Transformasi Silver

### Silver API — 4 Transformasi

| # | Transformasi | Alasan | Dampak pada Data |
|---|-------------|--------|-----------------|
| T1 | `dropDuplicates(["event_id"])` | Producer bisa restart dan mengirim event yang sama ke Kafka. Duplikat membuat frekuensi gempa per wilayah terlihat lebih tinggi dari kenyataan — Ternate bisa kelihatan lebih aktif padahal bukan. | Menghapus record ganda berdasarkan ID unik gempa |
| T2 | Filter `magnitude > 0 AND isNotNull` | Magnitude null atau ≤ 0 tidak bermakna secara seismologi. Nilai ini akan merusak perhitungan AVG, MAX, dan terutama Risk Score di Gold layer — rata-rata magnitude bisa jadi sangat rendah karena ada nilai 0. | Menghapus data yang tidak valid secara fisika |
| T3 | Cast ke `DoubleType` + parse timestamp ke `TimestampType` | Semua kolom dari JSON mentah tersimpan sebagai String. Kolom `magnitude` dan `depth_km` harus DoubleType untuk kalkulasi statistik. Timestamp harus TimestampType agar bisa digunakan Window Functions (lag, per jam) — ini yang membuka Gold 4 dan Gold 5. | Mengaktifkan analisis numerik dan temporal yang tidak mungkin dilakukan di ETS |
| T4 | Filter `depth_km >= 0 AND isNotNull` | Kedalaman gempa tidak mungkin negatif secara fisika — data ini corrupt. Nilai depth digunakan untuk kategori "Dangkal/Menengah/Dalam" yang menjadi salah satu komponen Risk Score. | Menghapus data corrupt, menjaga akurasi kategori kedalaman |

**Hasil cleaning API:** Bronze 30 record → Silver 30 record (data sudah bersih, 0 dihapus)

### Silver RSS — 3 Transformasi

| # | Transformasi | Alasan |
|---|-------------|--------|
| T1 | `dropDuplicates(["article_id"])` | RSS feed dari CNN Indonesia mengembalikan artikel yang sama di beberapa file JSON karena consumer berjalan berkali-kali. Tanpa dedup, satu artikel bisa terhitung berkali-kali di analisis Gold. |
| T2 | Filter `title isNotNull AND length > 5` | Artikel tanpa judul atau dengan judul sangat pendek (misal: "–") tidak berguna untuk join dengan data gempa. |
| T3 | Parse `published` ke `TimestampType` | Diperlukan untuk join temporal dengan data gempa di Gold 4 — berita harus bisa dibandingkan waktunya dengan timestamp gempa. |

**Hasil cleaning RSS:** Bronze 419 record → Silver 211 record (**208 duplikat dihapus**, 49.6%)

---

## 📈 Perbandingan Gold Layer vs Analisis ETS

### Gold 1 — Distribusi Magnitudo ✅ Repro ETS

| Aspek | ETS | Gold 1 |
|-------|-----|--------|
| Sumber data | JSON mentah HDFS | Silver (sudah di-cast ke DoubleType) |
| Akurasi AVG | Bisa salah jika magnitude tersimpan sebagai String | Akurat — DoubleType dari Silver |
| Duplikat | Bisa ada | Sudah dihapus di Silver |

**Output:**
```
+------------+------+-------------+-------------+------------+----------+
|kategori_mag|jumlah|rata_rata_mag|mag_tertinggi|mag_terendah|persentase|
+------------+------+-------------+-------------+------------+----------+
|      Sedang|    23|         4.54|          4.9|         4.3|      76.7|
|        Kuat|     7|         5.27|          5.7|         5.0|      23.3|
+------------+------+-------------+-------------+------------+----------+
```

---

### Gold 2 — Wilayah Paling Aktif ✅ Repro ETS

| Aspek | ETS | Gold 2 |
|-------|-----|--------|
| Kolom tersedia | jumlah, avg magnitude | + `gempa_dangkal` (baru) |
| Kolom `gempa_dangkal` | ❌ Tidak ada — depth belum diekstrak | ✅ Ada — dari `kategori_depth` di Silver |

**Keunggulan:** Kolom `gempa_dangkal` membantu BPBD menentukan prioritas respons — gempa dangkal lebih berbahaya bagi penduduk di permukaan.

**Output (Top 5):**
```
+--------------------------+------------+-------------+-------------+----------+-------------+
|                   wilayah|jumlah_gempa|rata_rata_mag|mag_tertinggi|gempa_kuat|gempa_dangkal|
+--------------------------+------------+-------------+-------------+----------+-------------+
|        Ternate, Indonesia|           6|         4.67|          5.1|         1|            5|
|         Modisi, Indonesia|           5|          4.8|          5.1|         2|            4|
|Pante Makasar, Timor Leste|           2|          5.0|          5.5|         1|            0|
|     Lospalos, Timor Leste|           2|          4.4|          4.4|         0|            0|
|          Ambon, Indonesia|           2|          4.5|          4.5|         0|            0|
+--------------------------+------------+-------------+-------------+----------+-------------+
```

---

### Gold 3 — Risk Score per Wilayah 🆕 Enhanced (Tidak Ada di ETS)

**Formula:** `risk_score = frekuensi × avg_mag × (1 + pct_dangkal)`

**Mengapa tidak bisa dibuat di ETS:**
- `pct_dangkal` butuh kolom `depth_km` bertipe DoubleType — di ETS masih String
- Derived metric seperti `risk_score` butuh data yang sudah bersih dan bebas duplikat

**Output:**
```
+--------------------------+---------+-------+-----------+----------+
|                   wilayah|frekuensi|avg_mag|pct_dangkal|risk_score|
+--------------------------+---------+-------+-----------+----------+
|        Ternate, Indonesia|        6|   4.67|       0.83|     51.33|
|         Modisi, Indonesia|        5|    4.8|        0.8|      43.2|
|   Gunungsitoli, Indonesia|        2|    5.0|        1.0|      20.0|
|         Bitung, Indonesia|        2|   4.55|        1.0|      18.2|
|           Tual, Indonesia|        2|    4.6|        0.5|      13.8|
+--------------------------+---------+-------+-----------+----------+
```

**Insight:** Ternate paling berisiko (skor 51.33) bukan hanya karena paling sering (6 kejadian), tapi 83% gempanya dangkal — lebih berbahaya bagi penduduk dibanding Gunungsitoli yang magnitudenya lebih tinggi tapi frekuensinya lebih rendah.

---

### Gold 4 — Significant Alerts Cross-source Join 🆕 Enhanced (Tidak Ada di ETS)

**Mengapa tidak bisa dibuat di ETS:** Di ETS, API dan RSS dianalisis terpisah. Tidak ada join antar sumber. Untuk join temporal, timestamp harus bertipe `TimestampType` — yang baru tersedia setelah Silver layer.

**Cara kerja:** Gempa M>4.5 di-join dengan berita RSS yang terbit dalam 2 jam setelah gempa.

**Output:**
```
+----------+---------+--------------------------+-----------+---------------------+
|  event_id|magnitude|                   wilayah|alert_level|jumlah_berita_terkait|
+----------+---------+--------------------------+-----------+---------------------+
|us6000sr5z|      5.7|   Gunungsitoli, Indonesia| 🟡 WASPADA|                    0|
|us6000srrc|      5.5|Pante Makasar, Timor Leste| 🟡 WASPADA|                    0|
|us6000srn6|      5.3|        Abepura, Indonesia| 🟡 WASPADA|                    0|
+----------+---------+--------------------------+-----------+---------------------+
```

Total 16 gempa M>4.5 terdeteksi. `jumlah_berita_terkait = 0` karena data RSS adalah berita umum (CNN Indonesia), bukan khusus gempa — ini justru menunjukkan pipeline cross-source join berjalan benar.

---

### Gold 5 — Temporal Analysis + Window Function 🆕 Enhanced (Tidak Ada di ETS)

**Mengapa tidak bisa dibuat di ETS:** Timestamp masih String di ETS — tidak bisa di-extract jam, tidak bisa pakai `lag()` Window Function.

**Output aktivitas per jam:**
```
+---+------------+-------+-------+----------+-------------+
|jam|jumlah_gempa|avg_mag|max_mag|gempa_kuat|gempa_dangkal|
+---+------------+-------+-------+----------+-------------+
|  1|           3|    4.9|    5.3|         1|            3|
|  3|           1|    5.7|    5.7|         1|            1|
|  6|           2|    5.1|    5.1|         2|            1|
| 17|           3|   4.67|    4.9|         0|            2|
+---+------------+-------+-------+----------+-------------+
```

**Window Function — Tren per jam (lag comparison):**
```
+---+------------+-----------+-----------+--------+
|jam|jumlah_gempa|prev_jumlah|delta_gempa|    tren|
+---+------------+-----------+-----------+--------+
|  0|           1|       NULL|       NULL|       –|
|  1|           3|          1|          2|  ↑ Naik|
|  2|           1|          3|         -2| ↓ Turun|
|  3|           1|          1|          0|→ Stabil|
+---+------------+-----------+-----------+--------+
```

---

## ⏱️ Demonstrasi Time Travel (Simulasi Delta Lake Versioning)

Di `02_silver.py`, Time Travel didemonstrasikan dengan mekanisme folder snapshot:

**History tabel Silver API:**
```
 Ver  Waktu                      Operasi                  Baris
-----------------------------------------------------------------
   0  2026-05-24T17:46:25.203681Z  CLEAN_API                   30
```

**Distribusi SEBELUM update:**
```
+------------+-----+
|kategori_mag|count|
+------------+-----+
|      Sedang|   23|
|        Kuat|    7|
+------------+-----+
```

**Distribusi SESUDAH update** (set 'Unknown' → 'Mikro'):
```
+------------+-----+
|kategori_mag|count|
+------------+-----+
|      Sedang|   23|
|        Kuat|    7|
+------------+-----+
```

**Distribusi VERSI LAMA (v000) — dibaca dari snapshot:**
```
+------------+-----+
|kategori_mag|count|
+------------+-----+
|      Sedang|   23|
|        Kuat|    7|
+------------+-----+
```

**Cara kerja Time Travel:**
- Setiap kali data di-overwrite, versi lama otomatis disalin ke folder `_snapshot_*/v000/`
- Version log tersimpan di `_version_log/*.json` dengan metadata operasi dan jumlah baris
- Untuk membaca versi lama: `spark.read.parquet("../_snapshot_gempa_api/v000")`
- Di Delta Lake asli: `spark.read.format("delta").option("versionAsOf", 0).load(path)`

**Keuntungan Time Travel:**
- Bisa rollback jika ada kesalahan input data
- Audit trail — kapan data berubah dan oleh operasi apa
- Reproduksibilitas analisis — bisa re-run dengan data versi lama

---

## 💡 Refleksi: Keuntungan Lakehouse vs Simpan Langsung di HDFS/JSON

| Aspek | HDFS JSON (ETS) | Data Lakehouse (Tugas Ini) |
|-------|----------------|---------------------------|
| **Format** | JSON teks — boros storage, lambat dibaca | Parquet columnar — kompresi snappy, query lebih cepat |
| **Schema** | Tidak ketat — kolom bisa beda antar file | Schema enforcement — konsisten di semua file |
| **Tipe data** | Semua String — harus di-cast manual setiap analisis | Sudah benar sejak Silver — tidak perlu cast ulang |
| **Duplikat** | Tidak ada mekanisme pencegahan | Dihapus di Silver layer secara sistematis |
| **Versioning** | Tidak ada — update = overwrite, data lama hilang | Time Travel — semua versi tersimpan dan bisa diakses |
| **ACID** | Tidak ada — data bisa corrupt jika proses gagal | Overwrite atomik — gagal = tidak ada perubahan |
| **Analisis** | Terbatas — timestamp String, tidak bisa Window Function | Penuh — TimestampType memungkinkan lag, per jam, per hari |
| **Cross-source** | API dan RSS selalu terpisah | Bisa di-join berdasarkan waktu di Gold layer |

**Kesimpulan:**  
Sebelum (ETS), Spark hanya berperan sebagai "pembaca file JSON." Setelah upgrade ke Lakehouse, data mengalir melalui 3 layer dengan kualitas yang meningkat di setiap tahap — Bronze (raw + metadata), Silver (bersih + tipe benar), Gold (insight siap pakai). Risk Score dan cross-source join yang ada di Gold layer **sama sekali tidak bisa dibuat di pipeline ETS** karena keterbatasan tipe data dan tidak adanya versioning.

---

## 📁 Struktur Data yang Dihasilkan

```
lakehouse/
└── lakehouse_data/
    ├── bronze/
    │   ├── gempa_api/          ← 30 record (API raw + metadata)
    │   │   └── _version_log/   ← version history
    │   └── gempa_rss/          ← 419 record (RSS raw + metadata)
    ├── silver/
    │   ├── gempa_api/          ← 30 record (cleaned)
    │   ├── gempa_rss/          ← 211 record (208 duplikat dihapus)
    │   └── _snapshot_gempa_api/← snapshot untuk Time Travel
    │       └── v000/           ← versi sebelum update
    └── gold/
        ├── gempa_mag_dist/           ← ✅ Repro ETS: 2 record
        ├── gempa_region_rank/        ← ✅ Repro ETS: 10 record
        ├── gempa_risk_score/         ← 🆕 Enhanced: 10 record
        ├── gempa_significant_alerts/ ← 🆕 Enhanced: 16 record
        ├── gempa_hourly_activity/    ← 🆕 Enhanced: 18 record
        └── gempa_hourly_trend/       ← 🆕 Enhanced: 18 record
```
![alt text](<Screenshot 2026-05-25 004934.png>) ![alt text](<Screenshot 2026-05-25 004912.png>) ![alt text](<Screenshot 2026-05-25 004824.png>)
![alt text](<Screenshot 2026-05-25 005053.png>) ![alt text](<Screenshot 2026-05-25 005041.png>) ![alt text](<Screenshot 2026-05-25 005030.png>) ![alt text](<Screenshot 2026-05-25 005018.png>) ![alt text](<Screenshot 2026-05-25 005004.png>) ![alt text](<Screenshot 2026-05-25 004950.png>)