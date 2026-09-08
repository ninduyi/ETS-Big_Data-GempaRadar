"""
dashboard/app.py - GempaRadar
[NamaAnggota5]: Flask Dashboard — localhost:5000
Menampilkan: hasil Spark + data gempa live + berita terbaru
"""

import json
import os
from datetime import datetime
from flask import Flask, render_template, jsonify

app = Flask(__name__)

BASE_DIR  = os.path.dirname(__file__)
DATA_DIR  = os.path.join(BASE_DIR, "data")

def load_json(filename: str) -> dict | list:
    path = os.path.join(DATA_DIR, filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/data")
def api_data():
    """Endpoint utama yang diakses dashboard setiap 30 detik."""
    spark_results = load_json("spark_results.json")
    live_api      = load_json("live_api.json")
    live_rss      = load_json("live_rss.json")

    # Pastikan live_api dan live_rss adalah list
    if not isinstance(live_api, list):
        live_api = []
    if not isinstance(live_rss, list):
        live_rss = []

    return jsonify({
        "spark"    : spark_results,
        "live_api" : live_api[:15],   # 15 gempa terbaru
        "live_rss" : live_rss[:8],    # 8 berita terbaru
        "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    })


@app.route("/api/status")
def api_status():
    return jsonify({"status": "ok", "service": "GempaRadar Dashboard"})


if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    print("=" * 50)
    print("  🌋 GempaRadar Dashboard")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5000, debug=True)
