import os
import json
import hashlib
import requests
from datetime import datetime
from google.cloud import storage
import yaml

# =============================
# Paths & constants
# =============================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
STATE_FILE = os.path.join(PROJECT_ROOT, "src", "ingestion", "state", "ipc_hashes.json")


# =============================
# Config & state
# =============================

def load_settings():
    path = os.path.join(PROJECT_ROOT, "config", "settings.yaml")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# =============================
# Utils
# =============================

def checksum(path, algo="sha256"):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(url, dest):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    with open(dest, "wb") as f:
        f.write(r.content)


def upload_to_gcs(bucket_name, local_path, dest_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_path)

    print(f"UPLOADED → gs://{bucket_name}/{dest_path}")


# =============================
# Main ingestion logic
# =============================

def run():
    settings = load_settings()
    state = load_state()

    tmp_path = settings["local"]["tmp_path"]
    os.makedirs(tmp_path, exist_ok=True)

    bucket = settings["gcs"]["bucket_raw"]
    raw_prefix = settings["gcs"]["raw_prefix"]

    datasets = settings["sources"]["ipc"]["datasets"]
    today = datetime.today().strftime("%Y-%m-%d")

    for ds in datasets:
        name = ds["name"]
        url = ds["url"]
        filename = os.path.basename(url)

        print(f"\nProcessing dataset: {name}")
        print(f"URL: {url}")

        local_file = os.path.join(tmp_path, filename)

        download_file(url, local_file)

        file_hash = checksum(local_file)
        previous_hash = state.get(name)

        if previous_hash == file_hash:
            print("→ No changes detected. Skipping upload.")
            continue

        dest_path = (
            f"{raw_prefix}/{name}/"
            f"ingestion_date={today}/{filename}"
        )

        upload_to_gcs(bucket, local_file, dest_path)

        state[name] = file_hash
        save_state(state)

        print("✔ State updated")

    print("\nIngestion finished successfully.")


if __name__ == "__main__":
    run()
