# main.py
# ─────────────────────────────────────────────────────────────
# QDArchive Data Acquisition & Classification Pipeline
#
# HOW TO USE:
#   python main.py acquire     ← runs all scrapers (Part 1)
#   python main.py classify    ← runs the classifier (Part 2)
#   python main.py stats       ← shows database statistics
#   python main.py export      ← exports database to CSV
#   python main.py all         ← runs everything in order
#
# Before running for the first time:
#   pip install requests anthropic tqdm
# ─────────────────────────────────────────────────────────────

import sys
import os

# Make sure our modules can find each other
sys.path.insert(0, os.path.dirname(__file__))

from db import create_table, export_to_csv, print_summary
from scrapers.zenodo_scraper    import run as run_zenodo
from scrapers.osf_scraper       import run as run_osf
from scrapers.dataverse_scraper import run as run_dataverse
from classifier import classify_all, print_classification_stats
from config import MAX_PAGES


def run_acquisition():
    """Part 1: Download data from all repositories."""
    print("\n" + "="*55)
    print("  PART 1: DATA ACQUISITION")
    print("="*55)
    print(f"  Max pages per query: {MAX_PAGES}")
    print(f"  Scrapers: Zenodo, OSF, Harvard Dataverse")
    print("="*55)

    # Initialize database
    create_table()

    # Run each scraper in turn
    run_zenodo()
    run_osf()
    run_dataverse()

    # Show summary at the end
    print_summary()
    export_to_csv("metadata_export.csv")
    print("\n✅ Part 1 complete! Don't forget to:")
    print("   1. git add . && git commit -m 'Part 1 done'")
    print("   2. git tag part-1-release")
    print("   3. git push && git push --tags")


def run_classification():
    """Part 2: Classify all downloaded records using ISIC Rev. 5."""
    print("\n" + "="*55)
    print("  PART 2: CLASSIFICATION")
    print("="*55)

    classify_all()
    print_classification_stats()
    export_to_csv("metadata_classified_export.csv")
    print("\n✅ Part 2 complete!")


def show_stats():
    """Show current database and classification statistics."""
    print_summary()
    print_classification_stats()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nUsage: python main.py [acquire|classify|stats|export|all]")
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "acquire":
        run_acquisition()

    elif command == "classify":
        create_table()
        run_classification()

    elif command == "stats":
        show_stats()

    elif command == "export":
        export_to_csv("metadata_export.csv")
        print("✅ Exported to metadata_export.csv")

    elif command == "all":
        run_acquisition()
        run_classification()

    else:
        print(f"Unknown command: {command}")
        print("Usage: python main.py [acquire|classify|stats|export|all]")
        sys.exit(1)


if __name__ == "__main__":
    main()
