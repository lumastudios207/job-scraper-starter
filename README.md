# Weekly Job Snapshot Starter

This starter project is a modular pipeline for collecting weekly job listings from multiple sites and exporting a snapshot CSV.

## What it does
- Reads one config file per site from `sites/`
- Uses Firecrawl to extract jobs into a shared schema
- Normalizes fields into one CSV
- Optionally uploads the CSV to Google Drive
- Makes it easy to add new sites later by adding a new config file

## Project structure
- `sites/`: one YAML config per source site
- `schemas/job_listing_schema.json`: shared extraction schema
- `runner.py`: main script
- `.env.example`: environment variable template

## Fields in the CSV
- `run_date`
- `source_site`
- `job_url`
- `company_name`
- `location`
- `is_remote`
- `posted_date_raw`

## Setup
1. Create a Firecrawl account and get an API key.
2. Create a Google Drive folder for weekly snapshots.
3. Copy `.env.example` to `.env` and fill in values.
4. Install Python packages:
   - `pip install requests pyyaml python-dotenv pandas google-api-python-client google-auth google-auth-oauthlib google-auth-httplib2`
5. Place OAuth credentials for Google Drive in the project folder if you want upload enabled.
6. Run `python runner.py`.

## Google Drive
Two options:
- Simpler: skip upload at first and manually place the CSV into Drive.
- Better: enable Drive upload with OAuth credentials and set `GOOGLE_DRIVE_FOLDER_ID`.

## Add a new site
1. Copy an existing file in `sites/`
2. Update the URL, extraction prompt, and rules
3. Run the script again

## Notes
- This starter keeps snapshot-only CSVs.
- It preserves the raw posted date text from the source sites.
- Remote jobs should still keep a location string when the site provides one.
