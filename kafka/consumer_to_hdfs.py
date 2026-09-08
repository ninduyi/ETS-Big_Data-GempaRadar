"""
consumer_to_hdfs.py - GempaRadar
[NamaAnggota3]: Consumer Kafka → simpan ke HDFS dan file lokal untuk dashboard
Membaca dari: gempa-api dan gempa-rss
Menyimpan ke: HDFS /data/gempa/api/ dan /data/gempa/rss/
Salinan lokal: dashboard/data/live_api.json dan dashboard/data/live_rss.json
"""

import json
import os
import time
import logging
import subprocess
import threading
from datetime import datetime, timezone
from kafka import KafkaConsumer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = "localhost:9092"
TOPIC_API         = "gempa-api"
TOPIC_RSS         = "gempa-rss"
GROUP_ID          = "gemparadar-hdfs-consumer"
FLUSH_INTERVAL    = 120   # flush ke HDFS setiap 2 menit
MAX_BUFFER        = 50    # atau setiap 50 event, mana lebih dulu

HDFS_BASE         = "/data/gempa"
HDFS_API_PATH     = f"{HDFS_BASE}/api"
HDFS_RSS_PATH     = f"{HDFS_BASE}/rss"
HDFS_HASIL_PATH   = f"{HDFS_BASE}/hasil"

LOCAL_TMP_DIR     = "/tmp/gemparadar"
DASHBOARD_DIR     = os.path.join(os.path.dirname(__file__), "..", "dashboard", "data")

LIVE_API_MAX      = 20
LIVE_RSS_MAX      = 10

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Buffer & State ────────────────────────────────────────────────────────────
buffer_api: list = []
buffer_rss: list = []
live_api:   list = []
live_rss:   list = []
lock = threading.Lock()


def ensure_dirs():
    """Pastikan direktori lokal tersedia."""
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    os.makedirs(DASHBOARD_DIR, exist_ok=True)

    # Buat direktori HDFS via docker exec
    for path in [HDFS_API_PATH, HDFS_RSS_PATH, HDFS_HASIL_PATH]:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", path],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            log.info(f"HDFS dir OK: {path}")
        else:
            log.warning(f"HDFS mkdir {path}: {result.stderr.strip()}")


def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


def save_to_hdfs(buffer: list, hdfs_path: str, label: str):
    """Simpan buffer ke file JSON di HDFS via docker cp + docker exec."""
    if not buffer:
        return

    ts        = now_ts()
    filename  = f"{label}_{ts}.json"
    local_tmp = os.path.join(LOCAL_TMP_DIR, filename)

    # Tulis ke file lokal dulu
    with open(local_tmp, "w", encoding="utf-8") as f:
        json.dump(buffer, f, ensure_ascii=False, indent=2)

    # Copy file ke dalam container namenode
    copy_result = subprocess.run(
        ["docker", "cp", local_tmp, f"namenode:/tmp/{filename}"],
        capture_output=True, text=True
    )
    if copy_result.returncode != 0:
        log.error(f"✗ Gagal copy ke container: {copy_result.stderr.strip()}")
        return

    # Upload dari container ke HDFS
    hdfs_target = f"{hdfs_path}/{filename}"
    put_result = subprocess.run(
        ["docker", "exec", "namenode", "hdfs", "dfs", "-put", "-f",
         f"/tmp/{filename}", hdfs_target],
        capture_output=True, text=True
    )
    if put_result.returncode == 0:
        log.info(f"✓ Tersimpan ke HDFS: {hdfs_target} ({len(buffer)} event)")
    else:
        log.error(f"✗ Gagal upload ke HDFS: {put_result.stderr.strip()}")

    # Bersihkan file tmp lokal
    os.remove(local_tmp)


def update_live_json(live_list: list, filepath: str):
    """Update file JSON lokal untuk dashboard."""
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(live_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.error(f"Gagal update live JSON {filepath}: {e}")


def flush_buffers():
    """Flush buffer ke HDFS dan update live JSON untuk dashboard."""
    global buffer_api, buffer_rss

    with lock:
        if buffer_api:
            save_to_hdfs(buffer_api, HDFS_API_PATH, "api")
            buffer_api = []

        if buffer_rss:
            save_to_hdfs(buffer_rss, HDFS_RSS_PATH, "rss")
            buffer_rss = []

        live_api_path = os.path.join(DASHBOARD_DIR, "live_api.json")
        live_rss_path = os.path.join(DASHBOARD_DIR, "live_rss.json")
        update_live_json(live_api, live_api_path)
        update_live_json(live_rss, live_rss_path)
        log.info(f"Live JSON diupdate: {len(live_api)} API, {len(live_rss)} RSS events")


def flush_loop():
    """Thread terpisah: flush ke HDFS setiap FLUSH_INTERVAL detik."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        log.info("=== Auto-flush ke HDFS ===")
        flush_buffers()


def consume_api():
    """Consumer untuk topic gempa-api."""
    log.info(f"Consumer API dimulai → topic: {TOPIC_API}")
    consumer = KafkaConsumer(
        TOPIC_API,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"{GROUP_ID}-api",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
session_timeout_ms=60000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
    )

    while True:
        try:
            for msg in consumer:
                data = msg.value
                with lock:
                    buffer_api.append(data)
                    live_api.insert(0, data)
                    if len(live_api) > LIVE_API_MAX:
                        live_api.pop()

                log.info(
                    f"[API] Diterima: Mag {data.get('magnitude')} | "
                    f"{data.get('place', '')}"
                )

                if len(buffer_api) >= MAX_BUFFER:
                    log.info(f"Buffer API penuh ({MAX_BUFFER}), flush sekarang")
                    flush_buffers()

        except Exception as e:
            log.error(f"Consumer API error: {e}")
            time.sleep(5)


def consume_rss():
    """Consumer untuk topic gempa-rss."""
    log.info(f"Consumer RSS dimulai → topic: {TOPIC_RSS}")
    consumer = KafkaConsumer(
        TOPIC_RSS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"{GROUP_ID}-rss",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
session_timeout_ms=60000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
    )

    while True:
        try:
            for msg in consumer:
                data = msg.value
                with lock:
                    buffer_rss.append(data)
                    live_rss.insert(0, data)
                    if len(live_rss) > LIVE_RSS_MAX:
                        live_rss.pop()

                log.info(
                    f"[RSS] Diterima: [{data.get('source')}] "
                    f"{data.get('title', '')[:60]}..."
                )

                if len(buffer_rss) >= MAX_BUFFER:
                    log.info(f"Buffer RSS penuh ({MAX_BUFFER}), flush sekarang")
                    flush_buffers()

        except Exception as e:
            log.error(f"Consumer RSS error: {e}")
            time.sleep(5)


def run():
    log.info("=== GempaRadar Consumer to HDFS dimulai ===")
    ensure_dirs()

    t_api   = threading.Thread(target=consume_api,  daemon=True, name="consumer-api")
    t_rss   = threading.Thread(target=consume_rss,  daemon=True, name="consumer-rss")
    t_flush = threading.Thread(target=flush_loop,   daemon=True, name="flush-loop")

    t_api.start()
    t_rss.start()
    t_flush.start()

    log.info("Semua thread berjalan. Tekan Ctrl+C untuk berhenti.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Berhenti... flush terakhir ke HDFS")
        flush_buffers()
        log.info("Consumer selesai.")


if __name__ == "__main__":
    run()
