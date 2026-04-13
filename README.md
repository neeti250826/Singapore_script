# Singapore GeBIZ Scraper

A Python Selenium scraper for Singapore GeBIZ opportunities, with strong support for `AWARDED` notices and item-level awarded contract extraction.

This project is based on the scraper logic provided in the working script shared by the user.

## Features

- Scrapes GeBIZ BOListing opportunities
- Supports `OPEN`, `CLOSED`, and `AWARDED`
- Extracts notice-level metadata
- Extracts award-level supplier and award details
- Extracts awarded item rows for goods contracts
- Expands one awarded notice into multiple CSV rows when multiple items exist
- Supports date filtering by `publication_date` or `closing_date`
- Supports early stop optimization for `publication_date`

## Output schema

The main CSV contains:

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

## Requirements

- Python 3.10+
- Google Chrome installed

Install dependencies:

```bash
pip install pandas beautifulsoup4 python-dateutil selenium webdriver-manager openpyxl
```

## Running the scraper

### Basic awarded scrape

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 5 --status-filter AWARDED
```

### Awarded scrape with publication date filter

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter AWARDED --date-from 2024-01-01 --date-to 2026-03-26 --date-field publication_date
```

### Closed scrape

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter CLOSED
```

## Batch execution

For large jobs, run batches of 50 or 100 pages.

### 50-page batches

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 51 --page-to 100 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 101 --page-to 150 --status-filter AWARDED
```

### 100-page batches

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 100 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 101 --page-to 200 --status-filter AWARDED
python Debug_Singapore.py --output-target . --page-from 201 --page-to 300 --status-filter AWARDED
```

### Batched + filtered

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 50 --status-filter AWARDED --date-from 2024-01-01 --date-to 2026-03-26 --date-field publication_date
python Debug_Singapore.py --output-target . --page-from 51 --page-to 100 --status-filter AWARDED --date-from 2024-01-01 --date-to 2026-03-26 --date-field publication_date
```

## Output files

Each run creates:

- `run_output_<status>_<page_suffix>.csv`
- `run_output_<status>_<page_suffix>.json`
- `gebiz_filtered_<status>_<page_suffix>.csv`
- `gebiz_filtered_<status>_<page_suffix>.json`

Example:

- `gebiz_filtered_awarded_p1_to_50.csv`

## Behavior notes

### Awarded rows

For awarded notices, the scraper may create multiple rows for one notice if the award contains multiple items.

### Contract period

`contract_period` is extracted from the overview tab before clicking the award tab.

### Item awarded value

`item_awarded_value` is stored without the currency suffix, such as `(SGD)`.

## Platform support

### Windows / PyCharm

The script uses `webdriver-manager` automatically.

### Linux / Colab

The script expects Chrome and ChromeDriver to exist in standard Linux paths.

## Recommended test command

Before large runs:

```bash
python Debug_Singapore.py --output-target . --page-from 1 --page-to 1 --status-filter AWARDED
```

## Troubleshooting

### Selenium driver error on Windows

Install:

```bash
pip install selenium webdriver-manager
```

### Missing award detail fields

Some GeBIZ detail pages may need reprocessing from `notice_url` using a repair script.

### GeBIZ selector changes

If tabs or fields stop working, review:

- listing page tab selectors
- award tab selectors
- item row parsing regexes
