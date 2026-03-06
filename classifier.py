# classifier.py
# ─────────────────────────────────────────────────────────────
# Part 2: Classification
#
# This script reads every record from the database and uses
# an LLM (via the Anthropic API) to classify it into the
# ISIC Rev. 5 taxonomy (Section → Division level).
#
# It also generates search tags for each record.
#
# Run this AFTER Part 1 is complete.
# ─────────────────────────────────────────────────────────────

import sqlite3
import json
import time
import re
from config import DB_PATH, REQUEST_DELAY_SECONDS

# ── Anthropic API ─────────────────────────────────────────────
# Install with: pip install anthropic
try:
    import anthropic
    CLIENT = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY from environment
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    print("[WARN] 'anthropic' package not installed. Run: pip install anthropic")

# ── ISIC Rev. 5 — Top-level sections (for reference in the prompt) ─
ISIC_SECTIONS = """
A - Agriculture, forestry and fishing
B - Mining and quarrying
C - Manufacturing
D - Electricity, gas, steam and air conditioning supply
E - Water supply; sewerage, waste management and remediation activities
F - Construction
G - Wholesale and retail trade; repair of motor vehicles and motorcycles
H - Transportation and storage
I - Accommodation and food service activities
J - Information and communication
K - Financial and insurance activities
L - Real estate activities
M - Professional, scientific and technical activities
N - Administrative and support service activities
O - Public administration and defence; compulsory social security
P - Education
Q - Human health and social work activities
R - Arts, entertainment and recreation
S - Other service activities
T - Activities of households as employers
U - Activities of extraterritorial organizations and bodies
"""


def build_prompt(title: str, description: str, file_type: str) -> str:
    """Build the classification prompt for the LLM."""
    return f"""You are classifying a qualitative research dataset into the ISIC Rev. 5 taxonomy.

ISIC Rev. 5 Sections:
{ISIC_SECTIONS}

Dataset to classify:
- Title: {title or 'Not provided'}
- Description: {description or 'Not provided'}
- File type: {file_type or 'Not provided'}

Instructions:
1. Choose the most appropriate ISIC Section (letter + name)
2. Choose the most appropriate Division within that section (2-digit code + name)
3. Generate 3–8 searchable tags (lowercase, comma-separated)

Respond ONLY with valid JSON in this exact format (no extra text):
{{
  "isic_section_code": "Q",
  "isic_section_name": "Human health and social work activities",
  "isic_division_code": "86",
  "isic_division_name": "Human health activities",
  "tags": "healthcare, nursing, patient experience, interviews, qualitative"
}}"""


def classify_record_with_llm(title: str, description: str, file_type: str) -> dict:
    """
    Send one record to the LLM for classification.
    Returns a dict with ISIC fields and tags, or empty dict on failure.
    """
    if not HAS_ANTHROPIC:
        return {}

    prompt = build_prompt(title, description, file_type)

    try:
        response = CLIENT.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()

        # Strip markdown code fences if present (e.g. ```json ... ```)
        raw = re.sub(r"^```(?:json)?|```$", "", raw, flags=re.MULTILINE).strip()

        result = json.loads(raw)
        return result

    except json.JSONDecodeError as e:
        print(f"  [ERR] JSON parse error: {e}")
        return {}
    except Exception as e:
        print(f"  [ERR] LLM call failed: {e}")
        return {}


def classify_all(limit: int = None):
    """
    Run classification on all unclassified records in the database.

    Parameters
    ----------
    limit : if set, only classify this many records (useful for testing)
    """
    if not HAS_ANTHROPIC:
        print("[ERROR] Cannot classify without the 'anthropic' package.")
        print("        Run: pip install anthropic")
        print("        Then set your API key: export ANTHROPIC_API_KEY=your_key_here")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Only fetch records that haven't been classified yet
    query = """
        SELECT id, title, description, file_type
        FROM downloads
        WHERE isic_section_code IS NULL
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = conn.execute(query).fetchall()
    print(f"\n[CLASSIFY] Found {len(rows)} unclassified records.\n")

    for i, row in enumerate(rows, 1):
        print(f"  [{i}/{len(rows)}] ID={row['id']} — {(row['title'] or '')[:50]}")

        result = classify_record_with_llm(
            title       = row["title"] or "",
            description = row["description"] or "",
            file_type   = row["file_type"] or "",
        )

        if result:
            conn.execute("""
                UPDATE downloads SET
                    isic_section_code  = :isic_section_code,
                    isic_section_name  = :isic_section_name,
                    isic_division_code = :isic_division_code,
                    isic_division_name = :isic_division_name,
                    tags               = :tags
                WHERE id = :id
            """, {**result, "id": row["id"]})
            conn.commit()
            print(f"         → {result.get('isic_section_code')} / {result.get('isic_division_code')} | tags: {result.get('tags', '')[:50]}")
        else:
            print(f"         → Classification failed, will retry later.")

        time.sleep(REQUEST_DELAY_SECONDS)

    conn.close()
    print(f"\n[DONE] Classification complete.")


def print_classification_stats():
    """Print a summary of how records are distributed across ISIC sections."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0]
    classified = conn.execute(
        "SELECT COUNT(*) FROM downloads WHERE isic_section_code IS NOT NULL"
    ).fetchone()[0]

    sections = conn.execute("""
        SELECT isic_section_code, isic_section_name, COUNT(*) as n
        FROM downloads
        WHERE isic_section_code IS NOT NULL
        GROUP BY isic_section_code
        ORDER BY n DESC
    """).fetchall()

    divisions = conn.execute("""
        SELECT isic_division_code, isic_division_name, COUNT(*) as n
        FROM downloads
        WHERE isic_division_code IS NOT NULL
        GROUP BY isic_division_code
        ORDER BY n DESC
        LIMIT 15
    """).fetchall()

    conn.close()

    print(f"\n{'='*55}")
    print(f"  CLASSIFICATION STATISTICS")
    print(f"{'='*55}")
    print(f"  Total records   : {total}")
    print(f"  Classified      : {classified}")
    print(f"  Unclassified    : {total - classified}")
    print(f"\n  Top ISIC Sections:")
    for row in sections:
        bar = "█" * min(row["n"], 30)
        print(f"    [{row['isic_section_code']}] {(row['isic_section_name'] or '')[:35]:35s} {row['n']:4d}  {bar}")

    print(f"\n  Top ISIC Divisions:")
    for row in divisions:
        print(f"    [{row['isic_division_code']}] {(row['isic_division_name'] or '')[:40]:40s} {row['n']:4d}")

    print(f"{'='*55}\n")


if __name__ == "__main__":
    # Quick test: classify just 5 records first
    print("Running classifier on first 5 records as a test...")
    classify_all(limit=5)
    print_classification_stats()
