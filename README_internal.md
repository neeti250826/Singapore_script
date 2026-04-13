# GeBIZ Awarded Scraper Documentation

This document explains how the `Debug_Singapore.py` scraper works, what it extracts, how to run it, and how to process large page ranges in batches.

## Overview

This scraper extracts Singapore GeBIZ opportunity data from:

`https://www.gebiz.gov.sg/ptn/opportunity/BOListing.xhtml?origin=menu`

It supports:

- `OPEN`
- `CLOSED`
- `AWARDED`

It is optimized for `AWARDED` notices and can extract both notice-level fields and awarded item-level fields into a single flat CSV structure.

## What the scraper extracts

The output schema includes these columns:

- `source`
- `country`
- `country_code`
- `publication_date`
- `closing_date`
- `title`
- `description`
- `buyer`
- `classification`
- `status`
- `currency`
- `amount`
- `awarding_agency_name`
- `supplier_name`
- `awarded_date`
- `awarded_value_detail`
- `contract_period`
- `item_no`
- `item_description`
- `item_uom`
- `item_quantity`
- `item_unit_price`
- `item_awarded_value`
- `notice_id`
- `notice_url`
- `query_text`
- `scraped_at_utc`
- `dedup_key`

## Special handling for AWARDED notices

For awarded notices, the scraper:

- extracts overview-tab data first
- then clicks the Award tab
- extracts award-specific fields
- expands one notice into multiple rows if multiple awarded items exist

That means one awarded notice can produce multiple rows in the CSV, one per awarded item.

### Contract period

`contract_period` is taken from the overview tab before clicking the award tab.

### Item-level awarded fields

For goods contracts, the scraper attempts to extract:

- `item_no`
- `item_description`
- `item_uom`
- `item_quantity`
- `item_unit_price`
- `item_awarded_value`

`item_awarded_value` has the currency suffix removed, for example:

- `1936.00 (SGD)` becomes `1936.00`

## Filtering behavior

The scraper supports:

- page range filtering
- status filtering
- query filtering
- date filtering

### Date filtering

Arguments:

- `--date-from`
- `--date-to`
- `--date-field`

Supported `date-field` values:

- `publication_date`
- `closing_date`

### Important optimization for publication date

When `--date-field publication_date` is used, the scraper does two optimizations:

1. It skips opening detail URLs for listing rows whose `publication_date` is outside the specified range.
2. It stops pagination early when all rows on the current listing page are older than `date_from`.

This improves speed significantly for large historical runs.

## How tab navigation works

The scraper:

1. Opens the BOListing page
2. Clicks the main status tab
3. For `AWARDED`, it first switches into `CLOSED`, then clicks the `AWARDED` subtab
4. Iterates through requested pages
5. Opens each detail page
6. Extracts fields from overview and award tabs

## Installation requirements

### Python version

Recommended:

- Python 3.10+
- Python 3.11 or 3.12 also work

### Python packages

Install these:

```bash
pip install pandas beautifulsoup4 python-dateutil selenium webdriver-manager openpyxl
```

### Browser requirements

The scraper uses Google Chrome.

#### Windows / PyCharm

The script uses `webdriver-manager` automatically on Windows.

No manual ChromeDriver setup is needed if Chrome is installed.

#### Linux / Colab

The script expects:

- `/usr/bin/google-chrome`
- `/usr/bin/chromedriver`

## How to run

Basic awarded run:

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 5 --status-filter AWARDED
```

Basic closed run:

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 5 --status-filter CLOSED
```

With publication date filter:

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter AWARDED --date-from 2024-01-01 --date-to 2026-03-26 --date-field publication_date
```

## Output files

The script creates four files for each run:

- `run_output_<status>_<page_suffix>.csv`
- `run_output_<status>_<page_suffix>.json`
- `gebiz_filtered_<status>_<page_suffix>.csv`
- `gebiz_filtered_<status>_<page_suffix>.json`

Examples:

- `run_output_awarded_p1_to_50.csv`
- `gebiz_filtered_awarded_p1_to_50.csv`

## Batch execution for large page ranges

For large runs, scrape in batches of 50 or 100 pages.

### Batch size: 50 pages

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 51 --page-to 100 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 101 --page-to 150 --status-filter AWARDED
```

### Batch size: 100 pages

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 100 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 101 --page-to 200 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 201 --page-to 300 --status-filter AWARDED
```

### Batch runs with date filter

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter AWARDED --date-from 2024-01-01 --date-to 2026-03-26 --date-field publication_date
python Debug_Singapore.py --output-target . --page-from 51 --page-to 100 --status-filter AWARDED --date-from 2024-01-01 --date-to 2026-03-26 --date-field publication_date
```

## Recommended workflow for batching

1. Run one batch at a time
2. Verify output row counts
3. Store each CSV separately
4. Merge files later if needed

Example merge in Python:

```python
import pandas as pd

files = [
    "gebiz_filtered_awarded_p1_to_50.csv",
    "gebiz_filtered_awarded_p51_to_100.csv",
    "gebiz_filtered_awarded_p101_to_150.csv",
]

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
df.to_csv("gebiz_filtered_awarded_merged.csv", index=False)
```

## Deduplication

The script generates `dedup_key` using:

- source
- notice_id
- notice_url
- item_no

That helps preserve one row per awarded item while preventing duplicates.

## Common issues

### 1. Selenium cannot find ChromeDriver on Windows

Install dependencies:

```bash
pip install selenium webdriver-manager
```

Make sure Google Chrome is installed.

### 2. Some award rows have missing details

This usually happens when:

- selectors changed on GeBIZ
- the Award tab did not click
- the notice structure differs from expected markup

Use a repair script to revisit `notice_url` and fill missing award details.

### 3. Wrong rows when repairing awarded items

Do not map item fields row-by-row unless item matching is exact.

The safer repair approach is:

- group by `notice_url`
- scrape once
- rebuild the awarded rows for that notice

## Suggested folder structure

```text
project/
├── Debug_Singapore.py
├── outputs/
│   ├── run_output_awarded_p1_to_50.csv
│   ├── gebiz_filtered_awarded_p1_to_50.csv
│   └── ...
└── docs/
    └── README_internal.md
```

## Maintenance notes

If GeBIZ changes its HTML:

- review tab selectors
- review Award detail selectors
- review item extraction regexes
- test on a single page first

## Quick test command

Use this first before large runs:

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 1 --status-filter AWARDED
```

Then scale up to 5, 50, or 100 pages.
