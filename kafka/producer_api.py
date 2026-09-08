"""
producer_api.py - GempaRadar
[NamaAnggota2]: Producer untuk API USGS Earthquake Real-time
Polling USGS setiap 5 menit, kirim ke Kafka topic: gempa-api
"""

import json
import time
import hashlib
import logging
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

# ── Konfigurasi ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_API       = "gempa-api"
POLL_INTERVAL   = 300  # 5 menit

# Bounding box Indonesia
USGS_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
    "?format=geojson"
    "&minlatitude=-11&maxlatitude=6"
    "&minlongitude=95&maxlongitude=141"
    "&minmagnitude=2"
    "&orderby=time"
    "&limit=30"
)

# Set untuk menghindari duplikat dalam satu sesi
sent_ids: set = set()

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER-API] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def create_producer() -> KafkaProducer:
    """Buat KafkaProducer dengan konfigurasi handal."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        enable_idempotence=True,
        acks="all",
        retries=5,
    )


def fetch_earthquakes() -> list[dict]:
    """Ambil data gempa terbaru dari USGS."""
    try:
        resp = requests.get(USGS_URL, timeout=15)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        log.info(f"Berhasil fetch {len(features)} gempa dari USGS")
        return features
    except Exception as e:
        log.error(f"Gagal fetch USGS: {e}")
        return []


def parse_feature(feature: dict) -> dict:
    """Parse satu GeoJSON feature menjadi dict yang konsisten."""
    props = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [None, None, None])

    # Konversi epoch ms → ISO timestamp
    epoch_ms = props.get("time")
    if epoch_ms:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        timestamp_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        timestamp_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Ekstrak nama wilayah: ambil bagian setelah " of " jika ada
    place_raw = props.get("place", "Unknown")
    if " of " in place_raw:
        wilayah = place_raw.split(" of ", 1)[1]
    else:
        wilayah = place_raw

    return {
        "event_id"   : feature.get("id", ""),
        "magnitude"  : props.get("mag"),
        "place"      : place_raw,
        "wilayah"    : wilayah,
        "timestamp"  : timestamp_iso,
        "longitude"  : coords[0],
        "latitude"   : coords[1],
        "depth_km"   : coords[2],
        "status"     : props.get("status", ""),
        "tsunami"    : props.get("tsunami", 0),
        "url"        : props.get("url", ""),
        "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source"     : "USGS",
    }


def run():
    log.info("=== GempaRadar Producer API dimulai ===")
    producer = create_producer()

    while True:
        features = fetch_earthquakes()
        new_count = 0

        for feature in features:
            event_id = feature.get("id", "")
            if not event_id or event_id in sent_ids:
                continue  # skip duplikat

            data = parse_feature(feature)
            key  = event_id  # key = ID gempa dari USGS

            try:
                future = producer.send(TOPIC_API, key=key, value=data)
                future.get(timeout=10)
                sent_ids.add(event_id)
                new_count += 1
                log.info(
                    f"Terkirim → ID: {event_id} | "
                    f"Mag: {data['magnitude']} | "
                    f"Lokasi: {data['place']}"
                )
            except Exception as e:
                log.error(f"Gagal kirim {event_id}: {e}")

        log.info(f"Siklus selesai: {new_count} event baru dikirim. "
                 f"Total sesi: {len(sent_ids)} event")
        producer.flush()
        log.info(f"Menunggu {POLL_INTERVAL} detik...")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
