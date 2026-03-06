# downloader.py
# ─────────────────────────────────────────────────────────────
# Shared helper functions for downloading files.
# All scrapers (Zenodo, OSF, etc.) use these functions.
# ─────────────────────────────────────────────────────────────

import os
import time
import requests
from pathlib import Path
from config import BASE_DOWNLOAD_DIR, REQUEST_DELAY_SECONDS


def make_local_dir(source_name: str, project_slug: str) -> str:
    """
    Create a local directory for a downloaded project and return its path.

    Directory structure:
        downloads/
            zenodo/
                doctor-nurse-study-xyz/
            osf/
                another-study-abc/

    Parameters
    ----------
    source_name  : e.g. "zenodo", "osf", "dataverse"
    project_slug : a short unique name for the project (letters + dashes only)
    """
    # Sanitize the slug: replace spaces with dashes, lowercase, strip odd chars
    safe_slug = "".join(
        c if c.isalnum() or c in "-_" else "-"
        for c in project_slug.lower()
    ).strip("-")[:80]  # max 80 chars to keep paths short

    dir_path = os.path.join(BASE_DOWNLOAD_DIR, source_name, safe_slug)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(source_name, safe_slug)   # relative path (for DB)


def download_file(url: str, local_dir_relative: str, filename: str) -> bool:
    """
    Download a single file from a URL and save it locally.

    Parameters
    ----------
    url                : Direct download URL of the file
    local_dir_relative : Relative path like "zenodo/my-study-123"
    filename           : What to name the file locally

    Returns
    -------
    bool : True if download succeeded, False otherwise
    """
    # Build the full path where we'll save the file
    full_dir = os.path.join(BASE_DOWNLOAD_DIR, local_dir_relative)
    os.makedirs(full_dir, exist_ok=True)
    full_path = os.path.join(full_dir, filename)

    # Skip if already downloaded
    if os.path.exists(full_path):
        print(f"  [SKIP] Already exists: {filename}")
        return True

    try:
        print(f"  [GET]  {filename} ← {url[:70]}...")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()  # raises an error if status != 200

        with open(full_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(full_path) // 1024
        print(f"  [OK]   Saved {size_kb} KB → {full_path}")
        time.sleep(REQUEST_DELAY_SECONDS)  # be polite to the server!
        return True

    except requests.exceptions.RequestException as e:
        print(f"  [ERR]  Failed to download {filename}: {e}")
        return False


def get_file_extension(filename: str) -> str:
    """Return the lowercased extension of a filename, e.g. '.qdpx'"""
    return Path(filename).suffix.lower()


def is_qda_file(filename: str, qda_extensions: list) -> bool:
    """Return True if the file has a known QDA extension."""
    return get_file_extension(filename) in qda_extensions


def sanitize_filename(name: str) -> str:
    """Remove characters that aren't safe in filenames."""
    return "".join(
        c if c.isalnum() or c in ".-_()" else "_"
        for c in name
    )[:120]
