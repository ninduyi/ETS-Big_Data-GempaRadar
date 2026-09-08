# 🌋 GempaRadar — Monitor Aktivitas Seismik Indonesia

**Mata Kuliah:** Big Data dan Data Lakehouse  
**Jenis Evaluasi:** Evaluasi Tengah Semester (ETS) — Praktik Kelompok  
**Topik:** Topik 6 — GempaRadar  

---

## Video Demo 

[![Demo ETS Big Data GempaRadar](https://img.youtube.com/vi/5AFBZIGzTqE/hqdefault.jpg)](http://www.youtube.com/watch?v=5AFBZIGzTqE "Demo ETS Big Data GempaRadar")

---

## 👥 Anggota Kelompok & Kontribusi

| No | Nama | NRP | Kontribusi |
|----|------|-----|------------|
| 1 | Mohammad Abyan Ranuaji | 5027241106 | Setup Docker (Hadoop & Kafka), konfigurasi infrastruktur |
| 2 | Muhammad Farrel Rafli Al Fasya | 5027241075 | `kafka/producer_api.py` — integrasi USGS API |
| 3 | Salomo | 5027221063 | `kafka/producer_rss.py` + `kafka/consumer_to_hdfs.py` |
| 4 | Nabilah Anindya Paramesti | 5027241006 | `spark/analysis.ipynb` — 3 analisis wajib |
| 5 | Ivan Syarifuddin | 5027241045 | `dashboard/app.py` + `dashboard/templates/index.html` |

---

## 🎯 Deskripsi Topik

GempaRadar adalah sistem monitoring aktivitas seismik Indonesia secara real-time. Sistem ini menjawab pertanyaan bisnis:

> **"Di wilayah mana aktivitas gempa paling tinggi dalam periode ini, dan seberapa sering gempa signifikan (M>4) terjadi?"**

**Sumber Data:**
- **API:** USGS Earthquake FDSN API (gratis, tanpa API key, data real-time seluruh Indonesia)
- **RSS:** BMKG Gempa M≥5.0 + Tempo Gempa Bumi

---

## 🏗️ Arsitektur Sistem

```
[USGS API]              [BMKG RSS + Tempo RSS]
     │                          │
     ▼                          ▼
producer_api.py          producer_rss.py
     │                          │
     ▼                          ▼
┌──────────────────────────────────────────┐
│             APACHE KAFKA                  │
│  Topic: gempa-api    Topic: gempa-rss     │
└──────────────────────┬───────────────────┘
                       │
                       ▼
               consumer_to_hdfs.py
                       │
                       ▼
┌──────────────────────────────────────────┐
│           HADOOP HDFS                     │
│  /data/gempa/api/   /data/gempa/rss/     │
│  /data/gempa/hasil/                       │
└──────────────┬───────────────────────────┘
               │
               ▼
         spark/analysis.ipynb
               │
               ▼
        dashboard/app.py
        localhost:5000
```

---

## ⚙️ Cara Menjalankan (Step-by-Step)

### Prasyarat
```bash
pip install kafka-python feedparser requests flask pyspark
```

### Step 1: Buat Docker Network
```bash
docker network create bigdata-network
```

### Step 2: Jalankan Hadoop
```bash
docker compose -f docker-compose-hadoop.yml up -d
```

Tunggu ±30 detik, lalu verifikasi:
```bash
# Cek container aktif (harus ada namenode, datanode1, datanode2, resourcemanager, nodemanager)
docker compose -f docker-compose-hadoop.yml ps

# Buat direktori HDFS
docker exec namenode hdfs dfs -mkdir -p /data/gempa/api
docker exec namenode hdfs dfs -mkdir -p /data/gempa/rss
docker exec namenode hdfs dfs -mkdir -p /data/gempa/hasil
docker exec namenode hdfs dfs -chmod -R 777 /data/gempa

# Verifikasi
docker exec namenode hdfs dfs -ls -R /data/
```

HDFS Web UI: [http://localhost:9870](http://localhost:9870)

### Step 3: Jalankan Kafka
```bash
docker compose -f docker-compose-kafka.yml up -d
```

Tunggu ±20 detik, lalu buat topic:
```bash
# Buat topic gempa-api
docker exec kafka-broker kafka-topics \
  --create --topic gempa-api \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Buat topic gempa-rss
docker exec kafka-broker kafka-topics \
  --create --topic gempa-rss \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# Verifikasi
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092
```
jika ada error saat pembuatan topic jalankan ini dan jalankan ulang pembuatan topic
```bash
docker-compose -f docker-compose-kafka.yml down
docker-compose -f docker-compose-kafka.yml up -d
```
Kafka UI: [http://localhost:8080](http://localhost:8080)

### Step 4: Jalankan Producer API (terminal 1)
```bash
python kafka/producer_api.py
```

Verifikasi event masuk:
```bash
docker exec kafka-broker kafka-console-consumer \
  --topic gempa-api --from-beginning --bootstrap-server localhost:9092
```

### Step 5: Jalankan Producer RSS (terminal 2)
```bash
python kafka/producer_rss.py
```

Verifikasi:
```bash
docker exec kafka-broker kafka-console-consumer \
  --topic gempa-rss --from-beginning --bootstrap-server localhost:9092
```

### Step 6: Jalankan Consumer to HDFS (terminal 3)
```bash
python kafka/consumer_to_hdfs.py
```

Verifikasi (setelah 2 menit):
```bash
docker exec namenode hdfs dfs -ls -R /data/gempa/
docker exec namenode hdfs dfs -du -h /data/gempa/api/
```

### Step 7 — Jalankan Spark Analysis (Otomatis)

```bash
# Install Spark terlebih dahulu jika belum ada
pip install pyspark
```
```bash
python3 spark/analysis.py
```

Script ini berjalan otomatis setiap 10 menit — sync data dari HDFS,
jalankan 3 analisis, dan update spark_results.json untuk dashboard.
Tidak perlu buka Jupyter manual.

### Step 8: Jalankan Dashboard
```bash
python dashboard/app.py
```

Buka browser: [http://localhost:5000](http://localhost:5000)

---

## 🔍 Verifikasi Checklist

### Kafka
```bash
# Cek semua topic
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092

# Cek consumer group
docker exec kafka-broker kafka-consumer-groups \
  --describe --group gemparadar-hdfs-consumer-api \
  --bootstrap-server localhost:9092
```

### HDFS
```bash
docker exec namenode hdfs dfs -ls -R /data/gempa/
docker exec namenode hdfs dfs -du -h /data/gempa/api/
```

### Dashboard
```bash
curl http://localhost:5000/api/status
curl http://localhost:5000/api/data
```

---

## 📊 Analisis Spark (3 Analisis Wajib)

| # | Analisis | Method | Output |
|---|----------|--------|--------|
| 1 | Distribusi Magnitudo (Mikro/Minor/Sedang/Kuat) | DataFrame API | Jumlah + persentase per kategori |
| 2 | Top 10 Wilayah Paling Aktif Seismik | Spark SQL | Ranking wilayah + rata-rata magnitude |
| 3 | Distribusi Kedalaman (Dangkal/Menengah/Dalam) | Spark SQL | Jumlah + statistik per kategori kedalaman |

---

## 📸 Screenshots

> - Screenshot HDFS Web UI (localhost:9870) menampilkan file di /data/gempa
<img width="1914" height="958" alt="image" src="https://github.com/user-attachments/assets/2a46c00f-14b2-447e-970b-9d66072f0713" />
<img width="1904" height="965" alt="image" src="https://github.com/user-attachments/assets/d3094c48-238c-4398-a326-ec4317d27cca" />

> - Screenshot output terminal producer_api.py (event gempa masuk)
<img width="1462" height="597" alt="image" src="https://github.com/user-attachments/assets/7766305f-184c-4b7d-b270-ee4ab314e123" />
> - Screenshot output terminal producer_rss.py
<img width="1437" height="605" alt="image" src="https://github.com/user-attachments/assets/49c8b5d8-b7fa-40ba-b20a-9084f007584b" />

> - Screenshot output terminal consumer (flush ke HDFS)
<img width="1438" height="632" alt="image" src="https://github.com/user-attachments/assets/1b376318-348b-4aea-889b-55669da79ade" />

> - Screenshot dashboard (localhost:5000) dengan data nyata
<img width="1919" height="863" alt="image" src="https://github.com/user-attachments/assets/65878f7a-9dfd-4414-8820-a26c80466722" />

> - Screenshot Spark output 3 analisis
<img width="1414" height="645" alt="image" src="https://github.com/user-attachments/assets/c7758f5d-08eb-4431-a8e9-506690bfa5dc" />
<img width="1456" height="314" alt="image" src="https://github.com/user-attachments/assets/6cc0b8bc-75df-4bf6-92d7-c2daa7a79f25" />


---

## 🧗 Tantangan & Solusi

| Tantangan | Solusi |
|-----------|--------|
| USGS API mengembalikan gempa yang sama berulang | Simpan `sent_ids` set dalam memori, skip jika sudah terkirim |
| Consumer harus baca 2 topic sekaligus | Gunakan `threading` — 1 thread per topic + 1 thread flush |
| Spark membaca banyak file JSON sekaligus | `spark.read.json(folder/)` otomatis membaca semua file dalam folder |
| Dashboard perlu data live + data Spark | Consumer simpan salinan lokal `live_api.json`; Flask baca keduanya |

---

## 📁 Struktur Repository

```
gemparadar-ets-bigdata/
├── README.md
├── docker-compose-hadoop.yml
├── docker-compose-kafka.yml
├── hadoop.env
├── kafka/
│   ├── producer_api.py
│   ├── producer_rss.py
│   └── consumer_to_hdfs.py
├── spark/
│   └── analysis.ipynb
│   └── analysis.py
└── dashboard/
    ├── app.py
    ├── templates/
    │   └── index.html
    └── data/               ← .gitignore (auto-generated)
        ├── spark_results.json
        ├── live_api.json
        └── live_rss.json
```

---

## 📝 Catatan Teknis

- **Rate limit USGS:** Tidak ada — API publik, data diperbarui setiap menit
- **Format data:** Semua event disimpan sebagai JSON dengan field `timestamp` konsisten (ISO 8601 UTC)
- **Idempotency:** Producer menggunakan `enable_idempotence=True` + set `sent_ids` untuk menghindari duplikat
- **Threading:** Consumer menggunakan 3 thread: 1 consume API, 1 consume RSS, 1 auto-flush ke HDFS
