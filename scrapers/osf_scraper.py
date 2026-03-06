# scrapers/osf_scraper.py
# ─────────────────────────────────────────────────────────────
# Scraper for OSF — Open Science Framework (https://osf.io)
# OSF is very popular for qualitative research projects.
# Its API is open and free, no key needed for public data.
#
# OSF structure:
#   Projects → Nodes → Files → Download URLs
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

OSF_SEARCH_API = "https://api.osf.io/v2/search/"
OSF_FILES_API  = "https://api.osf.io/v2/nodes/{node_id}/files/osfstorage/"
SOURCE_NAME = "osf"

HEADERS = {
    "User-Agent": "QDArchive-Student-Scraper/1.0 (university research project)"
}


def search_osf(query: str, page: int = 1) -> dict:
    """Search OSF for public projects matching a query."""
    params = {
        "q":        query,
        "page":     page,
        "per_page": PAGE_SIZE,
        "filter[type]": "project",
    }
    try:
        r = requests.get(OSF_SEARCH_API, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"  [ERR] OSF search failed: {e}")
        return {}


def get_osf_files(node_id: str) -> list:
    """
    Fetch the list of files in an OSF project node.
    Returns a flat list of file dicts with name + download URL.
    """
    url = OSF_FILES_API.format(node_id=node_id)
    files = []
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            links = item.get("links", {})

            if attrs.get("kind") == "file":
                files.append({
                    "name":         attrs.get("name", "unknown"),
                    "download_url": links.get("download", ""),
                    "size":         attrs.get("size", 0),
                })

    except requests.exceptions.RequestException as e:
        print(f"  [ERR] Couldn't get files for node {node_id}: {e}")

    return files


def process_node(node: dict):
    """Download all files from one OSF project node."""
    attrs    = node.get("attributes", {})
    node_id  = node.get("id", "unknown")

    title       = attrs.get("title", node_id)
    description = attrs.get("description", "")[:1000]
    date_str    = attrs.get("date_created", "")
    year        = date_str[:4] if date_str else ""
    license_data = attrs.get("node_license", {}) or {}
    license_str  = license_data.get("name", "") if license_data else ""

    source_url = f"https://osf.io/{node_id}/"

    print(f"\n  Project : {title[:60]}")
    print(f"  License : {license_str or 'NOT FOUND'}")

    # Get files in this project
    files = get_osf_files(node_id)
    if not files:
        print(f"  [SKIP] No accessible files.")
        return

    print(f"  Files   : {len(files)}")

    local_dir = make_local_dir(SOURCE_NAME, title or node_id)
    has_primary = False

    for f in files:
        file_url  = f["download_url"]
        file_name = sanitize_filename(f["name"])
        file_ext  = os.path.splitext(file_name)[1].lower()

        if not file_url:
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
            "title":           title,
            "description":     description,
            "year":            year,
            "license":         license_str,
            "source_name":     "OSF",
            "source_url":      source_url,
            "has_primary_data": 1 if has_primary else 0,
        })

    time.sleep(REQUEST_DELAY_SECONDS)


def run(max_pages: int = MAX_PAGES):
    """Search OSF with all configured queries and download results."""
    print(f"\n{'='*55}")
    print(f"  OSF SCRAPER")
    print(f"{'='*55}")

    total = 0

    for query in SEARCH_QUERIES:
        print(f"\n[QUERY] \"{query}\"")

        for page in range(1, max_pages + 1):
            print(f"  Fetching page {page}...")
            data = search_osf(query, page)

            nodes = data.get("data", [])
            if not nodes:
                print(f"  No more results on page {page}.")
                break

            for node in nodes:
                process_node(node)
                total += 1

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[DONE] OSF scraper finished. Processed {total} projects.")


if __name__ == "__main__":
    from db import create_table
    create_table()
    run(max_pages=2)
