from __future__ import annotations
import os
import json
import time
from pathlib import Path
from datetime import datetime
import requests
import yaml
import pandas as pd
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
SITES_DIR = BASE_DIR / "sites"
SCHEMA_PATH = BASE_DIR / "schemas" / "job_listing_schema.json"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

load_dotenv(BASE_DIR / ".env")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
GOOGLE_DRIVE_UPLOAD = os.getenv("GOOGLE_DRIVE_UPLOAD", "false").lower() == "true"
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")


def load_schema() -> dict:
    return json.loads(SCHEMA_PATH.read_text())


def load_site_configs() -> list[dict]:
    configs = []
    for path in sorted(SITES_DIR.glob("*.yaml")):
        configs.append(yaml.safe_load(path.read_text()))
    return configs


def _extract_jobs_from_data(data: object) -> list[dict]:
    """Safely extract job listings from various response shapes."""
    if isinstance(data, dict):
        if isinstance(data.get("jobs"), list):
            return data["jobs"]
        for value in data.values():
            found = _extract_jobs_from_data(value)
            if found:
                return found
    if isinstance(data, list):
        for item in data:
            found = _extract_jobs_from_data(item)
            if found:
                return found
    return []


def _firecrawl_headers() -> dict:
    return {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json",
    }


def _poll_extract(site_name: str, job_id: str) -> dict:
    """Poll GET /v1/extract/{id} until completed, failed, or 120s timeout."""
    poll_url = f"https://api.firecrawl.dev/v1/extract/{job_id}"
    auth = {"Authorization": f"Bearer {FIRECRAWL_API_KEY}"}
    deadline = time.monotonic() + 120
    poll_count = 0

    while time.monotonic() < deadline:
        time.sleep(2)
        poll_count += 1
        resp = requests.get(poll_url, headers=auth, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        status = result.get("status", "unknown")

        if status == "completed":
            print(f"  [{site_name}] completed after {poll_count} polls")
            return result
        if status == "failed":
            raise RuntimeError(f"Extraction failed: {json.dumps(result)[:300]}")
        if poll_count % 5 == 0:
            print(f"  [{site_name}] still {status} (poll {poll_count})...")

    raise TimeoutError(f"Extraction timed out after 120s (last status: {result.get('status')})")


def _submit_extract(site_name: str, payload: dict) -> dict:
    """POST /v1/extract and handle sync or async response."""
    print(f"  [{site_name}] POST /v1/extract started")
    response = requests.post(
        "https://api.firecrawl.dev/v1/extract",
        headers=_firecrawl_headers(),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()

    if result.get("status") == "completed" or (
        result.get("data") and result.get("status") != "processing"
    ):
        print(f"  [{site_name}] completed (sync)")
        return result

    job_id = result.get("id")
    if not job_id:
        raise RuntimeError(f"No job id in response: {json.dumps(result)[:200]}")

    print(f"  [{site_name}] async job {job_id}, polling...")
    return _poll_extract(site_name, job_id)


def _scrape_extract_one(site_name: str, url: str, config: dict, schema: dict, retries: int = 2) -> tuple:
    """Scrape+extract a single URL via /v1/scrape, with retries on timeout."""
    scrape_opts = config.get("scrape_options", {})

    payload = {
        "url": url,
        "formats": scrape_opts.get("formats", ["markdown", "extract"]),
        "extract": {
            "prompt": config["extract_prompt"],
            "schema": schema,
        },
    }
    if scrape_opts.get("onlyMainContent") is not None:
        payload["onlyMainContent"] = scrape_opts["onlyMainContent"]
    if scrape_opts.get("waitFor"):
        payload["waitFor"] = scrape_opts["waitFor"]

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                "https://api.firecrawl.dev/v1/scrape",
                headers=_firecrawl_headers(),
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
            result = resp.json()
            data = result.get("data") or {}
            extract = data.get("extract", {})
            return result, _extract_jobs_from_data(extract)
        except (requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            last_err = e
            if attempt < retries:
                wait = 5 * attempt
                print(f"  [{site_name}] attempt {attempt} failed ({e}), retrying in {wait}s...")
                time.sleep(wait)
    raise last_err


def firecrawl_scrape_extract(config: dict, schema: dict) -> list[dict]:
    """Use /v1/scrape with extract for single or multi-URL scrape+extract."""
    site_name = config["site_name"]
    urls = config.get("start_urls") or [config["start_url"]]

    all_jobs = []
    all_debug = {}
    seen_urls = set()

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(2)
        label = f"{site_name}:{i+1}/{len(urls)}" if len(urls) > 1 else site_name
        print(f"  [{label}] POST /v1/scrape (scrape+extract) {url}")
        try:
            result, jobs = _scrape_extract_one(site_name, url, config, schema)
            all_debug[url] = result
            # Deduplicate by job_url
            for job in jobs:
                jurl = job.get("job_url", "")
                if jurl and jurl not in seen_urls:
                    seen_urls.add(jurl)
                    all_jobs.append(job)
                elif not jurl:
                    all_jobs.append(job)
            print(f"  [{label}] got {len(jobs)} jobs ({len(all_jobs)} total unique)")
        except Exception as e:
            print(f"  [{label}] failed: {e}")
            all_debug[url] = {"error": str(e)}

    _save_debug(site_name, all_debug)
    return all_jobs


def firecrawl_extract(config: dict, schema: dict) -> list[dict]:
    if not FIRECRAWL_API_KEY:
        raise RuntimeError("Missing FIRECRAWL_API_KEY in .env")

    strategy = config.get("strategy", "extract")

    if strategy == "scrape_extract":
        return firecrawl_scrape_extract(config, schema)

    # Default: async /v1/extract
    site_name = config["site_name"]
    payload = {
        "urls": [config["start_url"]],
        "prompt": config["extract_prompt"],
        "schema": schema,
    }

    result = _submit_extract(site_name, payload)
    _save_debug(site_name, result)
    return _extract_jobs_from_data(result.get("data"))


def _save_debug(site_name: str, data: dict) -> None:
    path = OUTPUT_DIR / f"debug-{site_name}.json"
    path.write_text(json.dumps(data, indent=2))


def normalize_jobs(jobs: list[dict], config: dict, run_date: str) -> list[dict]:
    remote_keywords = [k.lower() for k in config.get("remote_keywords", [])]
    normalized = []
    for job in jobs:
        location = job.get("location")
        location_str = location.strip() if isinstance(location, str) else ""
        is_remote = bool(job.get("is_remote", False))
        if location_str and any(k in location_str.lower() for k in remote_keywords):
            is_remote = True
        normalized.append({
            "run_date": run_date,
            "source_site": config.get("source_site_label", config["site_name"]),
            "job_url": job.get("job_url", ""),
            "company_name": job.get("company_name", "").strip(),
            "location": location_str,
            "is_remote": is_remote,
            "posted_date_raw": (job.get("posted_date_raw") or "").strip(),
        })
    return normalized


def write_csv(rows: list[dict], run_date: str) -> Path:
    df = pd.DataFrame(rows, columns=[
        "run_date", "source_site", "job_url", "company_name", "location", "is_remote", "posted_date_raw"
    ])
    out_path = OUTPUT_DIR / f"weekly-job-snapshot-{run_date}.csv"
    df.to_csv(out_path, index=False)
    return out_path


def upload_to_google_drive(file_path: Path):
    if not GOOGLE_DRIVE_UPLOAD:
        return
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_UPLOAD is true but GOOGLE_DRIVE_FOLDER_ID is missing")
    raise NotImplementedError(
        "Google Drive upload is intentionally stubbed in this starter. "
        "Next step is to add OAuth credentials and Drive API upload logic."
    )


def main():
    run_date = datetime.utcnow().date().isoformat()
    schema = load_schema()
    configs = load_site_configs()
    all_rows = []

    for config in configs:
        try:
            jobs = firecrawl_extract(config, schema)
            rows = normalize_jobs(jobs, config, run_date)
            all_rows.extend(rows)
            print(f"Processed {config['site_name']}: {len(rows)} rows")
        except Exception as e:
            print(f"Failed {config['site_name']}: {e}")

    csv_path = write_csv(all_rows, run_date)
    print(f"Wrote CSV: {csv_path}")

    if GOOGLE_DRIVE_UPLOAD:
        upload_to_google_drive(csv_path)


if __name__ == "__main__":
    main()
