# config.py
# ─────────────────────────────────────────────────────────────
# Central configuration for the QDArchive scraper project.
# Edit the values here to change behaviour across the whole project.
# ─────────────────────────────────────────────────────────────

import os

# ── Where all downloaded files will be saved ──────────────────
BASE_DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")

# ── SQLite database file ──────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "metadata.sqlite")

# ── Known QDA file extensions (add more if you discover them) ─
QDA_EXTENSIONS = [
    ".qdpx",   # REFI-QDA standard (used by ATLAS.ti, NVivo, MAXQDA, etc.)
    ".mx24",   # MAXQDA
    ".nvp",    # NVivo (older)
    ".nvpx",   # NVivo (newer)
    ".qda",    # generic QDA
    ".hpr",    # HyperRESEARCH
    ".f4p",    # f4analyse
    ".qde",    # QDAMiner
    ".rbproject",  # Raven's Eye
]

# ── Search queries to use across repositories ─────────────────
SEARCH_QUERIES = [
    "qdpx qualitative data",
    "qualitative data analysis interview transcripts",
    "NVivo qualitative research",
    "MAXQDA qualitative",
    "ATLAS.ti qualitative",
    "interview transcript qualitative open data",
]

# ── How many seconds to wait between API calls ────────────────
# This is important so we don't get blocked by repositories!
REQUEST_DELAY_SECONDS = 1.5

# ── How many results to fetch per search page ────────────────
PAGE_SIZE = 25

# ── Max pages to fetch per query (increase to get more data) ──
MAX_PAGES = 5
