# scrapers/zenodo_scraper.py
# ─────────────────────────────────────────────────────────────
# Scraper for Zenodo (https://zenodo.org)
# Zenodo has a free, open API — no API key needed.
#
# How it works:
#   1. Search Zenodo for qualitative research projects
#   2. For each result, get the list of files
#   3. Download all files into a local folder
#   4. Save metadata to the database
# ─────────────────────────────────────────────────────────────

import sys
import os
import time
from datetime import datetime, timezone

# Add parent directory to path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
from config import SEARCH_QUERIES, PAGE_SIZE, MAX_PAGES, REQUEST_DELAY_SECONDS, QDA_EXTENSIONS
from db import record_exists, insert_record
from downloader import make_local_dir, download_file, sanitize_filename

ZENODO_API = "https://zenodo.org/api/records"
SOURCE_NAME = "zenodo"


def search_zenodo(query: str, page: int = 1) -> dict:
    """
    Call the Zenodo search API and return the raw JSON response.

    Parameters
    ----------
    query : search string, e.g. "qdpx qualitative interview"
    page  : which page of results to fetch (starts at 1)
    """
    params = {
        "q":            query,
        "size":         PAGE_SIZE,
        "page":         page,
        "access_right": "open",      # only open-access records
        "type":         "dataset",   # focus on datasets (not just papers)
    }
    try:
        response = requests.get(ZENODO_API, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"  [ERR] Zenodo search failed: {e}")
        return {}


def extract_metadata(record: dict) -> dict:
    """
    Pull the useful fields out of a raw Zenodo API record.
    Returns a flat dictionary matching our database schema.
    """
    meta = record.get("metadata", {})

    # Authors: join multiple authors into one string
    creators = meta.get("creators", [])
    author_str = "; ".join(c.get("name", "") for c in creators)

    # License: Zenodo puts it under metadata.license.id
    license_info = meta.get("license", {})
    license_str = license_info.get("id", "") if isinstance(license_info, dict) else str(license_info)

    # Publication year
    pub_date = meta.get("publication_date", "")
    year = pub_date[:4] if pub_date else ""

    # DOI
    doi = record.get("doi", "") or meta.get("doi", "")

    return {
        "source_name": "Zenodo",
        "source_url":  f"https://zenodo.org/records/{record.get('id', '')}",
        "doi":         doi,
        "title":       meta.get("title", ""),
        "author":      author_str,
        "license":     license_str,
        "year":        year,
        "description": meta.get("description", "")[:1000],  # trim long descriptions
    }


def process_record(record: dict):
    """
    For a single Zenodo search result:
      - Extract metadata
      - Create a local directory
      - Download each file
      - Insert a row into the database for each downloaded file
    """
    record_id = record.get("id", "unknown")
    meta = extract_metadata(record)
    files = record.get("files", [])

    if not files:
        print(f"  [SKIP] No files in record {record_id}")
        return

    # Use the record title (or ID) as the local folder name
    slug = meta["title"] or str(record_id)
    local_dir = make_local_dir(SOURCE_NAME, slug)

    print(f"\n  Project : {meta['title'][:60]}")
    print(f"  License : {meta['license'] or 'NOT FOUND'}")
    print(f"  Files   : {len(files)}")

    # Track whether any QDA files were found in this project
    has_qda = False
    has_primary = False

    for file_info in files:
        file_url      = file_info.get("links", {}).get("self", "")
        file_name     = sanitize_filename(file_info.get("key", "unknown"))
        file_ext      = os.path.splitext(file_name)[1].lower()

        if not file_url:
            continue

        # Skip if already in database
        if record_exists(file_url):
            print(f"  [DB]   Already recorded: {file_name}")
            continue

        # Download the file
        success = download_file(file_url, local_dir, file_name)
        if not success:
            continue

        # Detect file role
        if file_ext in QDA_EXTENSIONS:
            has_qda = True
        if file_ext in [".pdf", ".doc", ".docx", ".txt", ".rtf", ".odt"]:
            has_primary = True

        # Insert a record into the database
        insert_record({
            **meta,
            "url":             file_url,
            "download_date":   datetime.now(timezone.utc).isoformat(),
            "local_dir":       local_dir,
            "local_filename":  file_name,
            "file_type":       file_ext,
            "has_primary_data": 1 if has_primary else 0,
        })

    if not has_qda:
        print(f"  [NOTE] No QDA files found in this project (primary data only)")


def run(max_pages: int = MAX_PAGES):
    """
    Main entry point: search Zenodo with all configured queries
    and download everything found.
    """
    print(f"\n{'='*55}")
    print(f"  ZENODO SCRAPER")
    print(f"{'='*55}")

    total_processed = 0

    for query in SEARCH_QUERIES:
        print(f"\n[QUERY] \"{query}\"")

        for page in range(1, max_pages + 1):
            print(f"  Fetching page {page}...")
            data = search_zenodo(query, page)

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                print(f"  No more results on page {page}.")
                break

            for record in hits:
                process_record(record)
                total_processed += 1

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n[DONE] Zenodo scraper finished. Processed {total_processed} records.")


if __name__ == "__main__":
    # You can run just this file to test: python scrapers/zenodo_scraper.py
    from db import create_table
    create_table()
    run(max_pages=2)  # use 2 pages for a quick test
