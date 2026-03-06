# QDArchive Scraper — Setup & Usage Guide

## Project Structure

```
qdarchive/
├── main.py              ← START HERE — runs everything
├── config.py            ← settings (edit to tune behaviour)
├── db.py                ← database helpers
├── downloader.py        ← shared file download logic
├── classifier.py        ← Part 2 ISIC classification
├── scrapers/
│   ├── zenodo_scraper.py
│   ├── osf_scraper.py
│   └── dataverse_scraper.py
├── downloads/           ← all downloaded files go here
│   ├── zenodo/
│   ├── osf/
│   └── dataverse/
├── metadata.sqlite      ← the database (created automatically)
└── metadata_export.csv  ← CSV export (created on demand)
```

---

## Step 1 — Install dependencies

```bash
pip install requests anthropic tqdm
```

---

## Step 2 — Run Part 1 (Acquisition)

```bash
python main.py acquire
```

This will:
- Search Zenodo, OSF, and Harvard Dataverse
- Download all matching files into `downloads/`
- Record metadata in `metadata.sqlite`
- Export a `metadata_export.csv`

To test with just a few results first, edit `config.py` and set `MAX_PAGES = 1`.

---

## Step 3 — Before Submission (March 15)

```bash
git add .
git commit -m "Part 1 complete"
git tag part-1-release
git push && git push --tags
```

---

## Step 4 — Run Part 2 (Classification)

First, set your Anthropic API key:

```bash
# On Mac/Linux:
export ANTHROPIC_API_KEY=your_key_here

# On Windows (Command Prompt):
set ANTHROPIC_API_KEY=your_key_here
```

Then run:

```bash
python main.py classify
```

This will classify every record in the database using ISIC Rev. 5.

---

## Useful commands

```bash
python main.py stats     # show database summary
python main.py export    # export database to CSV
python main.py all       # run acquisition + classification in one go
```

---

## Adjusting how much data is downloaded

Edit `config.py`:

| Setting | Default | Effect |
|---|---|---|
| `MAX_PAGES` | 5 | More pages = more data |
| `PAGE_SIZE` | 25 | Results per page |
| `SEARCH_QUERIES` | 6 queries | Add more queries for more data |
| `REQUEST_DELAY_SECONDS` | 1.5 | Don't set below 1.0 |

---

## Adding a new repository scraper

1. Copy `scrapers/zenodo_scraper.py` as a template
2. Adjust the API URL and response parsing
3. Import and call it from `main.py` inside `run_acquisition()`

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `ModuleNotFoundError: requests` | `pip install requests` |
| `ModuleNotFoundError: anthropic` | `pip install anthropic` |
| Download fails with 403 | File is not truly open access, skip it |
| Database locked error | Close any other program using the .sqlite file |
| Classifier returns empty | Check your `ANTHROPIC_API_KEY` is set correctly |
