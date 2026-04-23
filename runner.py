from __future__ import annotations
import os
import json
import re
import time
from pathlib import Path
from datetime import datetime
import requests
import yaml
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError

BASE_DIR = Path(__file__).resolve().parent
SITES_DIR = BASE_DIR / "sites"
SCHEMA_PATH = BASE_DIR / "schemas" / "job_listing_schema.json"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

load_dotenv(BASE_DIR / ".env")
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "")
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
TOKEN_PATH = BASE_DIR / "token.json"
CREDENTIALS_PATH = BASE_DIR / "credentials.json"
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


def _scrape_extract_one(site_name: str, url: str, config: dict, schema: dict) -> tuple:
    """Scrape+extract a single URL via /v1/scrape, with configurable retries."""
    scrape_opts = config.get("scrape_options", {})
    retries = config.get("retries", 2)
    retry_wait = config.get("retry_wait", 5)

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
                print(f"  [{site_name}] attempt {attempt}/{retries} failed ({e}), retrying in {retry_wait}s...")
                time.sleep(retry_wait)
    raise last_err


def firecrawl_scrape_extract(config: dict, schema: dict) -> list[dict]:
    """Use /v1/scrape with extract for single or multi-URL scrape+extract."""
    site_name = config["site_name"]
    urls = config.get("start_urls") or [config["start_url"]]

    all_jobs = []
    all_debug = {}
    seen_urls = set()
    succeeded = []
    failed = []

    category_delay = config.get("category_delay", 2)

    for i, url in enumerate(urls):
        if i > 0:
            time.sleep(category_delay)
        category = url.rsplit("/", 1)[-1] if len(urls) > 1 else url
        label = f"{site_name}:{i+1}/{len(urls)}" if len(urls) > 1 else site_name
        print(f"  [{label}] POST /v1/scrape (scrape+extract) {url}")
        try:
            result, jobs = _scrape_extract_one(site_name, url, config, schema)
            all_debug[url] = result
            for job in jobs:
                jurl = job.get("job_url", "")
                if jurl and jurl not in seen_urls:
                    seen_urls.add(jurl)
                    all_jobs.append(job)
                elif not jurl:
                    all_jobs.append(job)
            print(f"  [{label}] {category}: {len(jobs)} jobs ({len(all_jobs)} total unique)")
            succeeded.append(category)
        except Exception as e:
            print(f"  [{label}] {category}: FAILED ({e})")
            all_debug[url] = {"error": str(e)}
            failed.append(category)

    _save_debug(site_name, all_debug)

    if len(urls) > 1:
        total = len(urls)
        print(f"  [{site_name}] summary: {len(succeeded)}/{total} categories succeeded, {len(failed)} timed out")
        if failed:
            print(f"  [{site_name}] failed categories: {', '.join(failed)}")

    return all_jobs


def firecrawl_extract(config: dict, schema: dict) -> list[dict]:
    if not FIRECRAWL_API_KEY:
        raise RuntimeError("Missing FIRECRAWL_API_KEY in .env")

    strategy = config.get("strategy", "extract")

    if strategy == "scrape_parse":
        return firecrawl_scrape_parse(config, schema)

    if strategy == "scrape_custom":
        return firecrawl_scrape_custom(config, schema)

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


LOCATION_HINTS = [
    ",", "USA", "Remote", "UK", "India", "Japan", "Canada", "Germany",
    "France", "United", "Australia", "Brazil", "Mexico", "Spain",
    "Netherlands", "Singapore", "Israel", "Ireland", "Switzerland",
    "Sweden", "Korea", "China", "Italy", "Poland", "Portugal",
]
SKIP_LINE_PREFIXES = (
    "!", "[Read", "[Powered", "USD ", "EUR ", "GBP ", "CAD ", "AUD ",
)


def parse_getro_markdown(markdown: str) -> list[dict]:
    """Parse Getro job board markdown into structured job listings."""
    cards = re.split(r"(?=#### \[)", markdown)
    jobs = []
    for card in cards:
        m = re.match(r"#### \[([^\]]+)\]\(([^\)]+)\)", card)
        if not m:
            continue
        title = m.group(1)
        url = m.group(2).replace(r"\#", "#")
        lines = [l.strip() for l in card.split("\n") if l.strip()]

        company = ""
        location = ""
        date_raw = ""
        for line in lines[1:]:
            if line.startswith(SKIP_LINE_PREFIXES):
                continue
            cm = re.match(r"\[([^\]]+)\]\(", line)
            if cm and not company:
                company = cm.group(1)
                continue
            if re.match(r"^(Today|Yesterday|\d+ days?)$", line):
                date_raw = line
                continue
            if not location and any(h in line for h in LOCATION_HINTS):
                location = line
                continue

        jobs.append({
            "job_title": title,
            "company_name": company,
            "location": location,
            "is_remote": "remote" in location.lower() if location else False,
            "posted_date_raw": date_raw,
            "job_url": url,
        })
    return jobs


def firecrawl_scrape_parse(config: dict, schema: dict) -> list[dict]:
    """Scrape markdown with Load More click, then parse Getro cards.

    Getro boards show 20 cards initially with a Load More button.
    After one click (loading 20 more = 40 total), the button is replaced
    by an IntersectionObserver infinite scroll that Firecrawl cannot trigger.
    So we click once then fall back gracefully if it fails.
    """
    site_name = config["site_name"]
    url = config["start_url"]
    scrape_opts = config.get("scrape_options", {})
    retries = config.get("retries", 2)
    retry_wait = config.get("retry_wait", 5)

    # Try with Load More click first for 40 cards, fall back to 20 if it fails
    for try_click in [True, False]:
        payload = {
            "url": url,
            "formats": ["markdown"],
            "onlyMainContent": True,
            "waitFor": scrape_opts.get("waitFor", 3000),
        }
        if try_click:
            payload["actions"] = [
                {"type": "click", "selector": '[data-testid="load-more"]'},
                {"type": "wait", "milliseconds": 3000},
            ]

        print(f"  [{site_name}] POST /v1/scrape (scrape+parse{'+ Load More' if try_click else ''}) {url}")
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
                md = (result.get("data") or {}).get("markdown", "")
                jobs = parse_getro_markdown(md)
                print(f"  [{site_name}] parsed {len(jobs)} jobs")
                _save_debug(site_name, result)
                return jobs
            except requests.exceptions.HTTPError as e:
                if resp.status_code == 500 and "Element not found" in resp.text and try_click:
                    print(f"  [{site_name}] no Load More button, falling back to base page")
                    break  # break retry loop, outer loop will try without click
                last_err = e
                if attempt < retries:
                    print(f"  [{site_name}] attempt {attempt}/{retries} failed ({e}), retrying in {retry_wait}s...")
                    time.sleep(retry_wait)
            except requests.exceptions.Timeout as e:
                last_err = e
                if attempt < retries:
                    print(f"  [{site_name}] attempt {attempt}/{retries} timed out, retrying in {retry_wait}s...")
                    time.sleep(retry_wait)
        else:
            if last_err and not try_click:
                raise last_err
            if last_err:
                print(f"  [{site_name}] Load More failed, falling back to base page")

    raise RuntimeError(f"[{site_name}] all scrape attempts failed")


def parse_yc_markdown(markdown: str) -> list[dict]:
    """Parse Y Combinator job board markdown into structured job listings."""
    blocks = re.split(r"(?=\[!\[]\(https://bookface-images)", markdown)
    jobs = []
    for block in blocks:
        tm = re.search(
            r"\[([^\]]+)\]\((https://www\.ycombinator\.com/companies/[^/]+/jobs/[^)]+)\)",
            block,
        )
        if not tm:
            continue
        title = tm.group(1)
        job_url = tm.group(2)

        cm = re.search(
            r"\[([^•\]]+?)(?:\s*\([^)]+\))?\s*•[^\]]*?"
            r"\((\d+ days? ago|about \d+ \w+ ago|yesterday|today)\)",
            block,
            re.IGNORECASE,
        )
        company = re.sub(r"\s*\([^)]+\)\s*$", "", cm.group(1)).strip() if cm else ""
        date_raw = cm.group(2) if cm else ""

        title_idx = block.find(title)
        apply_idx = block.find("[Apply]")
        if apply_idx == -1:
            apply_idx = len(block)
        meta_section = block[title_idx + len(title) : apply_idx]
        meta_parts = [p.strip() for p in meta_section.split("•") if p.strip()]

        location = ""
        for part in reversed(meta_parts):
            part = part.strip()
            if not part or part.startswith("[") or part.startswith("!"):
                continue
            if re.match(r"^(Full-time|Part-time|Contract|Internship|Design|Engineering|UI|UX|Product|Web)", part):
                continue
            if re.match(r"^[\$€£₹¥]|^\d+[KMk]", part):
                continue
            location = part
            break

        is_remote = "remote" in location.lower() if location else False
        jobs.append({
            "job_title": title,
            "company_name": company,
            "location": location,
            "is_remote": is_remote,
            "posted_date_raw": date_raw,
            "job_url": job_url,
        })
    return jobs


def parse_uiuxjobsboard_markdown(markdown: str) -> list[dict]:
    """Parse uiuxjobsboard.com markdown into structured job listings."""
    jobs = []
    headings = re.finditer(
        r"###\s+\[(?:!\[[^\]]*\]\([^\)]*\)\\\s*)?([^\]]+)\]"
        r"\((https://uiuxjobsboard\.com/job/[^\)]+)\)",
        markdown,
    )
    for hm in headings:
        text = hm.group(1).strip()
        job_url = hm.group(2)

        parts = re.split(r"is\s*hiring", text, maxsplit=1)
        company = parts[0].strip() if len(parts) == 2 else ""
        title = parts[1].strip() if len(parts) == 2 else text

        pos = hm.end()
        next_heading = markdown.find("###", pos + 1)
        if next_heading < 0:
            next_heading = len(markdown)
        section = markdown[pos:next_heading]

        locs = re.findall(
            r"\[([^\]]+)\]\(https://uiuxjobsboard\.com/design-jobs/", section
        )
        location = ", ".join(locs) if locs else ""

        date_raw = ""
        dm = re.search(r"(?:^|\n)\s*(\d+h|\d+ days? ago|yesterday)\s*(?:\n|$)", section)
        if dm:
            date_raw = dm.group(1)

        is_remote = "remote" in location.lower()
        jobs.append({
            "job_title": title,
            "company_name": company,
            "location": location,
            "is_remote": is_remote,
            "posted_date_raw": date_raw,
            "job_url": job_url,
        })
    return jobs


def parse_uxcel_markdown(markdown: str) -> list[dict]:
    """Parse UXcel job board markdown into structured job listings."""
    blocks = re.split(r"(?=\]\(https://app\.uxcel\.com/jobs/)", markdown)
    jobs = []
    for block in blocks:
        um = re.match(r"\]\((https://app\.uxcel\.com/jobs/[^\)]+)\)", block)
        if not um:
            continue
        job_url = um.group(1)
        idx = markdown.find(block)
        start = markdown.rfind("[", 0, idx)
        if start < 0:
            continue
        content = markdown[start:idx]
        lines = [
            l.strip().strip("\\")
            for l in content.replace("\\n", "\n").split("\n")
            if l.strip().strip("\\")
        ]

        title = ""
        company = ""
        date_raw = ""
        location = ""
        for line in lines:
            line = line.strip()
            bm = re.match(r"\*\*(.+?)\*\*", line)
            if bm:
                title = bm.group(1)
                continue
            if re.search(r"\d+ days? ago", line):
                date_raw = line
                continue
            if "|" in line and re.search(r"Remote|Only|Anywhere", line, re.IGNORECASE):
                location = line.replace(r"\|", "|")
                continue
            if line in ("New", "Full-time", "Part-time", "Contract", "Freelance"):
                continue
            if line.startswith(("[", "!", "#")):
                continue
            if not company and len(line) > 1:
                company = line

        if title:
            is_remote = "remote" in location.lower() if location else False
            jobs.append({
                "job_title": title,
                "company_name": company,
                "location": location,
                "is_remote": is_remote,
                "posted_date_raw": date_raw,
                "job_url": job_url,
            })
    return jobs


def parse_dribbble_markdown(markdown: str) -> list[dict]:
    """Parse Dribbble job board markdown into structured job listings."""
    entries = re.split(r"\n\d{2}\.\s", markdown)
    jobs = []
    for entry in entries:
        tm = re.search(r"####\s+(.+)", entry)
        if not tm:
            continue
        title = tm.group(1).strip()

        um = re.search(r"\[View job\]\(([^\)]+)\)", entry)
        job_url = um.group(1) if um else ""

        lines = [l.strip() for l in entry.split("\n") if l.strip()]
        company = ""
        for line in lines:
            if line.startswith(("!", "[", "#", "Featured")):
                continue
            if line and len(line) < 80:
                company = line
                break

        title_idx = entry.find("####")
        view_idx = entry.find("[View job]")
        location = ""
        if title_idx >= 0 and view_idx >= 0:
            between = entry[title_idx:view_idx]
            for bl in [l.strip() for l in between.split("\n") if l.strip()][1:]:
                if bl and not bl.startswith(("#", "[")) and bl != "Apply now":
                    location = bl
                    break

        date_raw = ""
        dm = re.search(r"(Posted .+? ago)", entry)
        if dm:
            date_raw = dm.group(1)

        is_remote = "remote" in (location + " " + title).lower()
        jobs.append({
            "job_title": title,
            "company_name": company,
            "location": location,
            "is_remote": is_remote,
            "posted_date_raw": date_raw,
            "job_url": job_url,
        })
    return jobs


def parse_consider_markdown(markdown: str) -> list[dict]:
    """Parse Consider-powered job board markdown into structured job listings."""
    jobs = []
    current_company = ""
    lines = markdown.split("\n")

    for i, line in enumerate(lines):
        cm = re.search(r"\[!\[([^\]]+?) logo\]", line)
        if cm:
            current_company = cm.group(1)
            continue

        jm = re.match(r"\s*## \[([^\]]+)\]\(([^\)]+)\)", line)
        if not jm:
            continue
        title = jm.group(1)
        job_url = jm.group(2)

        location = ""
        date_raw = ""
        for j in range(i + 1, min(i + 8, len(lines))):
            dl = lines[j].strip()
            dm = re.search(r"(Posted .+? ago)", dl)
            if dm:
                date_raw = dm.group(1)
                loc = dl[: dm.start()].strip()
                if loc:
                    location = loc
                break

        is_remote = "remote" in location.lower() if location else False
        jobs.append({
            "job_title": title,
            "company_name": current_company,
            "location": location,
            "is_remote": is_remote,
            "posted_date_raw": date_raw,
            "job_url": job_url,
        })
    return jobs


def firecrawl_scrape_custom(config: dict, schema: dict) -> list[dict]:
    """Scrape markdown and parse with a site-specific parser."""
    site_name = config["site_name"]
    url = config["start_url"]
    scrape_opts = config.get("scrape_options", {})
    retries = config.get("retries", 2)
    retry_wait = config.get("retry_wait", 5)
    parser = config.get("parser")

    parsers = {
        "yc": parse_yc_markdown,
        "consider": parse_consider_markdown,
        "dribbble": parse_dribbble_markdown,
        "uxcel": parse_uxcel_markdown,
        "uiuxjobsboard": parse_uiuxjobsboard_markdown,
    }
    parse_fn = parsers.get(parser)
    if not parse_fn:
        raise RuntimeError(f"Unknown parser: {parser}")

    payload = {
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,
        "waitFor": scrape_opts.get("waitFor", 3000),
    }

    print(f"  [{site_name}] POST /v1/scrape (scrape+custom:{parser}) {url}")
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
            md = (result.get("data") or {}).get("markdown", "")
            jobs = parse_fn(md)
            print(f"  [{site_name}] parsed {len(jobs)} jobs")
            _save_debug(site_name, result)
            return jobs
        except (requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
            last_err = e
            if attempt < retries:
                print(f"  [{site_name}] attempt {attempt}/{retries} failed ({e}), retrying in {retry_wait}s...")
                time.sleep(retry_wait)
    raise last_err


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
            "job_title": (job.get("job_title") or "").strip(),
            "job_url": job.get("job_url", ""),
            "company_name": job.get("company_name", "").strip(),
            "location": location_str,
            "is_remote": is_remote,
            "posted_date_raw": (job.get("posted_date_raw") or "").strip(),
        })
    return normalized


_MD_ARTIFACT_RE = re.compile(r"!\[|]\([^)]*\)|\*\*|__|\[|]")


def _clean_markdown(value: str) -> str:
    """Strip common markdown artifacts from a string."""
    return _MD_ARTIFACT_RE.sub("", value).strip()


def clean_and_validate_rows(rows: list[dict]) -> list[dict]:
    """Clean markdown artifacts and remove rows with empty company_name."""
    cleaned = []
    for row in rows:
        for field in ("company_name", "job_title"):
            original = row[field]
            scrubbed = _clean_markdown(original)
            if scrubbed != original:
                print(f'  WARNING: Cleaned markdown artifacts from {field} in {row["source_site"]}: "{original}" → "{scrubbed}"')
                row[field] = scrubbed

        if not row["company_name"].strip():
            print(f'  WARNING: Removed row with empty company_name from {row["source_site"]}: {row["job_url"]}')
            continue
        cleaned.append(row)
    return cleaned


DESIGN_KEYWORDS = [
    "ux designer", "ux design", "product designer", "web designer",
    "ui designer", "ui engineer", "ui/ux", "ux/ui",
    "interaction designer", "visual designer", "experience designer",
    "design lead", "head of design", "creative director",
    "brand designer", "digital designer", "interface designer",
]


def filter_design_roles(rows: list[dict]) -> list[dict]:
    return [r for r in rows if any(k in r["job_title"].lower() for k in DESIGN_KEYWORDS)]


def write_csv(rows: list[dict], run_date: str) -> Path:
    df = pd.DataFrame(rows, columns=[
        "run_date", "source_site", "job_title", "company_name", "job_url", "location", "is_remote", "posted_date_raw"
    ])
    out_path = OUTPUT_DIR / f"weekly-job-snapshot-{run_date}.csv"
    df.to_csv(out_path, index=False)
    return out_path


def _print_token_expired_error(file_path: Path) -> None:
    print(
        "CRITICAL: Google Drive token expired or revoked.\n"
        "Please run: python refresh_token.py\n"
        f"CSV has been saved locally at: {file_path}\n"
        "Re-run python runner.py after refreshing the token."
    )


def get_drive_service():
    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                if TOKEN_PATH.exists():
                    TOKEN_PATH.unlink()
                raise
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDENTIALS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json())
    return build("drive", "v3", credentials=creds)


def upload_to_google_drive(file_path: Path) -> str | None:
    """Upload to Drive. Returns the webViewLink on success, None on failure/disabled.

    On RefreshError: deletes token.json, prints instructions, returns None
    so the pipeline can continue with the local CSV instead of crashing.
    """
    if not GOOGLE_DRIVE_UPLOAD:
        return None
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise RuntimeError("GOOGLE_DRIVE_UPLOAD is true but GOOGLE_DRIVE_FOLDER_ID is missing")
    try:
        service = get_drive_service()
        file_metadata = {
            "name": file_path.name,
            "parents": [GOOGLE_DRIVE_FOLDER_ID],
        }
        media = MediaFileUpload(str(file_path), mimetype="text/csv", resumable=True)
        uploaded = service.files().create(
            body=file_metadata, media_body=media, fields="id, name, webViewLink"
        ).execute()
        link = uploaded.get("webViewLink")
        print(f"Uploaded to Drive: {uploaded.get('name')} — {link}")
        return link
    except RefreshError:
        _print_token_expired_error(file_path)
        return None


def main():
    run_date = datetime.now().date().isoformat()
    schema = load_schema()
    configs = load_site_configs()
    all_rows = []
    site_results = []  # (site_name, extracted, kept, error)

    for config in configs:
        site_name = config["site_name"]
        try:
            jobs = firecrawl_extract(config, schema)
            rows = normalize_jobs(jobs, config, run_date)
            rows = clean_and_validate_rows(rows)
            extracted = len(rows)
            kept = filter_design_roles(rows)
            all_rows.extend(kept)
            print(f"  {site_name}: {extracted} extracted → {len(kept)} design roles kept")
            site_results.append((site_name, extracted, len(kept), None))
        except Exception as e:
            print(f"Failed {site_name}: {e}")
            site_results.append((site_name, 0, 0, str(e)))

    # Health check summary
    print()
    print("HEALTH CHECK SUMMARY:")
    warnings = 0
    for site_name, extracted, kept, error in site_results:
        if error:
            status = "FAILED"
            warnings += 1
        elif kept == 0:
            status = "WARNING"
            warnings += 1
        else:
            status = "OK"
        print(f"  {site_name}: {kept} design roles — {status}")
    ok_count = len(site_results) - warnings
    print(f"  Overall: {ok_count}/{len(site_results)} sites OK, {warnings} warnings")
    print()

    csv_path = write_csv(all_rows, run_date)
    print(f"Wrote CSV: {csv_path}")

    drive_msg = ""
    drive_ok = True
    if GOOGLE_DRIVE_UPLOAD:
        drive_ok = upload_to_google_drive(csv_path)
        drive_msg = " CSV uploaded to Drive." if drive_ok else " Drive upload skipped (token issue)."

    print(f"Run complete: {len(all_rows)} design roles across {len(site_results)} sites. Warnings: {warnings}.{drive_msg}")

    # ── Enrichment phase ─────────────────────────────────────────────
    print()
    print("--- STARTING ENRICHMENT ---")
    try:
        from enrich import run_enrichment
        run_enrichment()
    except Exception as e:
        print(f"Enrichment failed: {e}")


if __name__ == "__main__":
    main()
