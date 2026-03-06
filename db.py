# db.py
# ─────────────────────────────────────────────────────────────
# Handles everything related to the SQLite database:
#   - Creating the table
#   - Inserting a new record
#   - Checking for duplicates (so we don't download the same thing twice)
#   - Exporting to CSV
# ─────────────────────────────────────────────────────────────

import sqlite3
import csv
from config import DB_PATH


def get_connection():
    """Open (or create) the SQLite database and return a connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # lets us access columns by name
    return conn


def create_table():
    """
    Create the 'downloads' table if it doesn't exist yet.
    Safe to call multiple times — it won't overwrite existing data.
    """
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS downloads (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Required fields (professor's spec)
            url             TEXT NOT NULL,
            download_date   TEXT NOT NULL,
            local_dir       TEXT NOT NULL,
            local_filename  TEXT NOT NULL,

            -- Optional metadata
            file_type       TEXT,
            title           TEXT,
            author          TEXT,
            uploader_name   TEXT,
            uploader_email  TEXT,
            source_name     TEXT,   -- e.g. "Zenodo", "OSF", "Harvard Dataverse"
            source_url      TEXT,   -- landing page URL (not the file URL)
            doi             TEXT,
            license         TEXT,
            year            TEXT,
            description     TEXT,
            has_primary_data INTEGER DEFAULT 0,  -- 1 if interview files etc. found

            -- Classification fields (filled in Part 2)
            isic_section_code   TEXT,
            isic_section_name   TEXT,
            isic_division_code  TEXT,
            isic_division_name  TEXT,
            tags                TEXT   -- comma-separated search tags
        )
    """)
    conn.commit()
    conn.close()
    print(f"[DB] Table ready at: {DB_PATH}")


def record_exists(url: str) -> bool:
    """Return True if a record with this URL is already in the database."""
    conn = get_connection()
    row = conn.execute(
        "SELECT id FROM downloads WHERE url = ?", (url,)
    ).fetchone()
    conn.close()
    return row is not None


def insert_record(record: dict) -> int:
    """
    Insert a new download record into the database.

    Parameters
    ----------
    record : dict
        Keys should match column names. Only 'url', 'download_date',
        'local_dir', and 'local_filename' are required.

    Returns
    -------
    int
        The row ID of the newly inserted record.
    """
    conn = get_connection()
    cursor = conn.execute("""
        INSERT INTO downloads (
            url, download_date, local_dir, local_filename,
            file_type, title, author, uploader_name, uploader_email,
            source_name, source_url, doi, license, year, description,
            has_primary_data
        ) VALUES (
            :url, :download_date, :local_dir, :local_filename,
            :file_type, :title, :author, :uploader_name, :uploader_email,
            :source_name, :source_url, :doi, :license, :year, :description,
            :has_primary_data
        )
    """, {
        # Fill in defaults for any missing optional keys
        "url":              record.get("url", ""),
        "download_date":    record.get("download_date", ""),
        "local_dir":        record.get("local_dir", ""),
        "local_filename":   record.get("local_filename", ""),
        "file_type":        record.get("file_type"),
        "title":            record.get("title"),
        "author":           record.get("author"),
        "uploader_name":    record.get("uploader_name"),
        "uploader_email":   record.get("uploader_email"),
        "source_name":      record.get("source_name"),
        "source_url":       record.get("source_url"),
        "doi":              record.get("doi"),
        "license":          record.get("license"),
        "year":             record.get("year"),
        "description":      record.get("description"),
        "has_primary_data": record.get("has_primary_data", 0),
    })
    conn.commit()
    row_id = cursor.lastrowid
    conn.close()
    return row_id


def export_to_csv(output_path: str = "metadata_export.csv"):
    """Export the entire downloads table to a CSV file."""
    conn = get_connection()
    rows = conn.execute("SELECT * FROM downloads").fetchall()
    if not rows:
        print("[DB] No records to export yet.")
        conn.close()
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        # Write header row using column names
        writer.writerow(rows[0].keys())
        writer.writerows(rows)

    conn.close()
    print(f"[DB] Exported {len(rows)} records to: {output_path}")


def print_summary():
    """Print a quick summary of what's in the database."""
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
    sources = conn.execute(
        "SELECT source_name, COUNT(*) as n FROM downloads GROUP BY source_name"
    ).fetchall()
    conn.close()

    print(f"\n{'─'*40}")
    print(f"  DATABASE SUMMARY")
    print(f"{'─'*40}")
    print(f"  Total records : {total}")
    print(f"  By source:")
    for row in sources:
        print(f"    {row['source_name'] or 'Unknown':20s} → {row['n']} records")
    print(f"{'─'*40}\n")


if __name__ == "__main__":
    # Quick test: create table and print summary
    create_table()
    print_summary()
