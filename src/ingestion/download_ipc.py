import os
import re
import json
import hashlib
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def load_settings(path=None):
    if path is None:
        path = os.path.join(PROJECT_ROOT, "config", "settings.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_file_links(base_url, list_page_path, download_prefix):
    url = base_url + list_page_path
    resp = requests.get(url)
    resp.raise_for_status()

    print("STATUS CODE:", resp.status_code)
    print("=== HTML PREVIEW (primeros 2000 chars) ===")
    print(resp.text[:2000])
    print("=== END HTML PREVIEW ===")

    soup = BeautifulSoup(resp.text, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(download_prefix):
            links.append(href)

    return links


def match_patterns(filename, patterns):
    matched = False
    for pat in patterns:
        if re.search(pat, filename):
            print(f"MATCH OK: {filename} ~ {pat}")
            matched = True
        else:
            print(f"NO MATCH: {filename} ~ {pat}")
    return matched



def checksum(path, algo="sha256"):
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

STATE_FILE = "src/ingestion/state/ipc_hashes.json"

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

def extract_year_month_from_filename(filename):
    match = re.search(r"sh_ipc_(\d{1,2})_(\d{2})\.xls", filename)
    if not match:
        today = datetime.today()
        return today.year, today.month

    month = int(match.group(1))
    year = 2000 + int(match.group(2))

    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month in filename: {filename}")

    return year, month


def upload_to_gcs(bucket_name, local_path, dest_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)
    blob.upload_from_filename(local_path)
    print(f"Uploaded gs://{bucket_name}/{dest_path}")


def run():
    s = load_settings()

    state = load_state()

    links = get_file_links(
        s["base_url"],
        s["list_page_path"],
        s["download_path_prefix"],
    )

    print("LINKS ENCONTRADOS:")
    for l in links:
        print(l)

    os.makedirs(s["local_tmp_path"], exist_ok=True)

    for href in links:
        filename = os.path.basename(href)
        print(f"\nEvaluando archivo: '{filename}'")

        if not match_patterns(filename, s["file_patterns"]["ipc"]):
            continue

        file_url = s["base_url"] + href
        local_file = os.path.join(s["local_tmp_path"], filename)

        print(f"Downloading {filename}")
        r = requests.get(file_url)
        r.raise_for_status()

        with open(local_file, "wb") as f:
            f.write(r.content)

        file_hash = checksum(local_file)
        previous_hash = state.get(filename)

        if previous_hash == file_hash:
            print(f"Skipping {filename} (no changes)")
            continue

        year, month = extract_year_month_from_filename(filename)
        dest_path = f"{s['bucket_raw_path_prefix']}/{year}/{month:02d}/{filename}"

        upload_to_gcs(s["bucket_raw"], local_file, dest_path)

        state[filename] = file_hash
        save_state(state)


if __name__ == "__main__":
    run()
