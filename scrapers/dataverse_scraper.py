# scrapers/dataverse_scraper.py
# ─────────────────────────────────────────────────────────────
# Scraper for Harvard Dataverse (https://dataverse.harvard.edu)
# Also works for any other Dataverse installation.
# Open API, no key needed.
# ─────────────────────────────────────────────────────────────

import sys
import os
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
from config import SEARCH_QUERIES, PAGE_SIZE, MAX_PAGES, REQUEST_DELAY_SECONDS, QDA_EXTENSIONS
from db import record_exists, insert_record
from downloader import make_local_dir, download_file, sanitize_filename

BASE_URL    = "https://dataverse.harvard.edu"
SEARCH_API  = f"{BASE_URL}/api/search"
FILE_API    = f"{BASE_URL}/api/datasets/{{dataset_id}}/versions/:latest/files"
DOWNLOAD    = f"{BASE_URL}/api/access/datafile/{{file_id}}"
SOURCE_NAME = "dataverse"


def search_dataverse(query: str, start: int = 0) -> dict:
    """Search Harvard Dataverse for open datasets."""
    params = {
        "q":    query,
        "type": "dataset",
        "per_page": PAGE_SIZE,
        "start": start,
    }
    try:
        r = requests.get(SEARCH_API, params=params, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"  [ERR] Dataverse search failed: {e}")
        return {}


def get_dataset_files(dataset_id: str) -> list:
    """Get list of files for a dataset by its database ID."""
    url = FILE_API.format(dataset_id=dataset_id)
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        print(f"  [ERR] Couldn't get files for dataset {dataset_id}: {e}")
        return []


def process_dataset(item: dict):
    """Download all files from one Dataverse dataset."""
    name        = item.get("name", "")
    description = item.get("description", "")[:1000]
    url_str     = item.get("url", "")
    dataset_id  = item.get("entity_id", "")

    # Extract year from published_at or similar
    pub = item.get("published_at", "")
    year = pub[:4] if pub else ""

    # Dataverse stores license in a nested field
    license_str = item.get("license", {}).get("name", "") if isinstance(item.get("license"), dict) else ""

    print(f"\n  Dataset : {name[:60]}")
    print(f"  License : {license_str or 'NOT FOUND'}")

    if not dataset_id:
        print("  [SKIP] No dataset ID.")
        return

    files = get_dataset_files(dataset_id)
    print(f"  Files   : {len(files)}")

    if not files:
        return

    local_dir = make_local_dir(SOURCE_NAME, name or str(dataset_id))
    has_primary = False

    for f in files:
        df          = f.get("dataFile", {})
        file_id     = df.get("id", "")
        file_name   = sanitize_filename(df.get("filename", f"file_{file_id}"))
        file_ext    = os.path.splitext(file_name)[1].lower()
        file_url    = DOWNLOAD.format(file_id=file_id)

        if not file_id:
            continue
        if record_exists(file_url):
            print(f"  [DB]   Already recorded: {file_name}")
            continue

        success = download_file(file_url, local_dir, file_name)
        if not success:
            continue

        if file_ext in [".pdf", ".doc", ".docx", ".txt", ".rtf"]:
            has_primary = True

        insert_record({
            "url":             file_url,
            "download_date":   datetime.now(timezone.utc).isoformat(),
            "local_dir":       local_dir,
            "local_filename":  file_name,
            "file_type":       file_ext,
            "title":           name,
            "description":     description,
            "year":            year,
            "license":         license_str,
            "source_name":     "Harvard Dataverse",
            "source_url":      url_str,
            "has_primary_data": 1 if has_primary else 0,
        })

    time.sleep(REQUEST_DELAY_SECONDS)


def run(max_pages: int = MAX_PAGES):
    """Search Dataverse and download matching datasets."""
    print(f"\n{'='*55}")
    print(f"  HARVARD DATAVERSE SCRAPER")
    print(f"{'='*55}")

    total = 0

    for query in SEARCH_QUERIES:
        print(f"\n[QUERY] \"{query}\"")

        for page in range(max_pages):
            start = page * PAGE_SIZE
            print(f"  Fetching results {start}–{start + PAGE_SIZE}...")
            data = search_dataverse(query, start=start)

            items = data.get("data", {}).get("items", [])
            if not items:
                print("  No more results.")
                break

            for item in items:
                process_dataset(item)
                total += 1

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[DONE] Dataverse scraper finished. Processed {total} datasets.")


if __name__ == "__main__":
    from db import create_table
    create_table()
    run(max_pages=2)
