"""
enrich.py — Hunter API enrichment for weekly job snapshots.

Reads the latest weekly-job-snapshot CSV, deduplicates by company,
looks up decision-maker contacts via Hunter.io, verifies emails,
and produces an Instantly-ready outreach CSV.
"""
from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

# ── Paths & env ──────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
load_dotenv(BASE_DIR / ".env")

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")
GOOGLE_DRIVE_UPLOAD = os.getenv("GOOGLE_DRIVE_UPLOAD", "false").lower() == "true"

# Reuse Drive upload from runner.py
sys.path.insert(0, str(BASE_DIR))
from runner import upload_to_google_drive  # noqa: E402

# ── Config ───────────────────────────────────────────────────────────
TITLE_KEYWORDS = [
    # Design decision-makers
    "head of design", "vp design", "vp of design", "director of design",
    "chief design officer", "design director", "head of product design",
    # Product decision-makers
    "cpo", "chief product officer", "vp product", "vp of product",
    "head of product", "director of product", "product director",
    # Founders and executives
    "founder", "co-founder", "ceo", "chief executive officer",
    "cto", "chief technology officer",
]


# ── Helpers ──────────────────────────────────────────────────────────
def find_latest_snapshot() -> tuple[Path, str]:
    """Find the most recent weekly-job-snapshot CSV by filename date."""
    snapshots = sorted(OUTPUT_DIR.glob("weekly-job-snapshot-*.csv"))
    if not snapshots:
        raise FileNotFoundError("No weekly-job-snapshot CSVs found in output/")
    latest = snapshots[-1]
    # Extract date from filename
    date_str = latest.stem.replace("weekly-job-snapshot-", "")
    return latest, date_str


def deduplicate_companies(df: pd.DataFrame) -> pd.DataFrame:
    """One row per unique company with aggregated metadata."""
    grouped = df.groupby("company_name", sort=False).agg(
        roles_hiring_for=("job_title", lambda x: ", ".join(x.unique())),
        source_boards=("source_site", lambda x: ", ".join(x.unique())),
        sample_job_url=("job_url", "first"),
    ).reset_index()
    return grouped


_TITLE_PATTERNS = [re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE) for kw in TITLE_KEYWORDS]


def _matches_title(position: str) -> bool:
    pos = position or ""
    return any(p.search(pos) for p in _TITLE_PATTERNS)


def _hunter_get(endpoint: str, params: dict, company_label: str) -> dict | None:
    """Make a Hunter API GET request with rate-limit retry."""
    url = f"https://api.hunter.io/v2/{endpoint}"
    params["api_key"] = HUNTER_API_KEY
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            print(f"  Rate limited on {company_label}, waiting 10s...")
            time.sleep(10)
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                print(f"  Still rate limited for {company_label}, skipping.")
                return None
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Hunter API error for {company_label}: {e}")
        return None


def enrich_company(company_name: str) -> tuple[str | None, list[dict], str]:
    """Run Hunter Domain Search + Email Verify for one company.

    Returns (domain, list_of_verified_contacts, skip_reason).
    skip_reason is "" on success, or a key like "size_filtered", "no_domain", etc.
    """
    # Step A: Domain Search
    data = _hunter_get("domain-search", {"company": company_name, "limit": 10, "size": "1,100"}, company_name)
    if data is None:
        return None, [], "api_error"

    body = data.get("data", {})
    domain = body.get("domain") or body.get("webmail", "")
    emails = body.get("emails", [])

    if not domain and not emails:
        # No domain + no emails with size filter = likely outside target range
        print(f"  Skipped {company_name} — outside target size range (1-100 employees)")
        return None, [], "size_filtered"

    if not domain:
        print(f"  No domain found for {company_name}")
        return None, [], "no_domain"

    # Filter by title
    decision_makers = [e for e in emails if _matches_title(e.get("position", ""))]
    if not decision_makers:
        print(f"  No relevant decision-maker contacts for {company_name}")
        return domain, [], "no_decision_makers"

    # Step B: Email Verify each decision-maker
    verified = []
    for contact in decision_makers:
        email = contact.get("value", "")
        if not email:
            continue
        time.sleep(1)  # rate limit
        vdata = _hunter_get("email-verifier", {"email": email}, company_name)
        if vdata is None:
            continue
        vbody = vdata.get("data", {})
        result = vbody.get("result", "")
        score = vbody.get("score", 0)
        if result == "deliverable" or score >= 70:
            verified.append({
                "first_name": contact.get("first_name", ""),
                "last_name": contact.get("last_name", ""),
                "email": email,
                "title": contact.get("position", ""),
                "linkedin_url": contact.get("linkedin", "") or "",
            })
        else:
            print(f"  Contact {email} failed verification (result={result}, score={score})")

    if not verified and decision_makers:
        print(f"  All contacts failed verification for {company_name}")

    return domain, verified, ""


def run_enrichment(max_companies: int | None = None):
    """Main enrichment pipeline.

    Args:
        max_companies: If set, only process this many unique companies (test mode).
    """
    if not HUNTER_API_KEY:
        print("HUNTER_API_KEY not set — skipping enrichment step.")
        return None

    # ── Input ────────────────────────────────────────────────────────
    snapshot_path, snapshot_date = find_latest_snapshot()
    print(f"Reading snapshot: {snapshot_path.name}")
    df = pd.read_csv(snapshot_path)
    total_rows = len(df)
    companies = deduplicate_companies(df)
    total_companies = len(companies)

    if max_companies:
        companies = companies.head(max_companies)
        print(f"TEST MODE: limiting to first {max_companies} companies ({total_companies} total)")

    # ── Enrich ───────────────────────────────────────────────────────
    outreach_rows = []
    stats = {
        "domain_found": 0,
        "no_domain": 0,
        "size_filtered": 0,
        "contacts_verified": 0,
        "linkedin_found": 0,
    }

    for _, row in companies.iterrows():
        company = row["company_name"]
        roles = row["roles_hiring_for"]
        boards = row["source_boards"]

        print(f"\n  Processing: {company}")
        time.sleep(1)  # rate limit before domain-search call

        domain, verified, skip_reason = enrich_company(company)

        if skip_reason == "size_filtered":
            stats["size_filtered"] += 1
            continue
        elif domain:
            stats["domain_found"] += 1
        else:
            stats["no_domain"] += 1
            continue

        stats["contacts_verified"] += len(verified)

        for contact in verified:
            if contact["linkedin_url"]:
                stats["linkedin_found"] += 1
            outreach_rows.append({
                "first_name": contact["first_name"],
                "last_name": contact["last_name"],
                "email": contact["email"],
                "title": contact["title"],
                "linkedin_url": contact["linkedin_url"],
                "company_name": company,
                "website": domain,
                "roles_hiring_for": roles,
                "source_boards": boards,
                "job_posted": roles,
            })

    # ── Output CSV ───────────────────────────────────────────────────
    out_path = OUTPUT_DIR / f"outreach-{snapshot_date}.csv"
    out_df = pd.DataFrame(outreach_rows, columns=[
        "first_name", "last_name", "email", "title", "linkedin_url",
        "company_name", "website", "roles_hiring_for", "source_boards", "job_posted",
    ])
    out_df.to_csv(out_path, index=False)

    # ── Drive upload ─────────────────────────────────────────────────
    drive_status = "SKIPPED"
    if GOOGLE_DRIVE_UPLOAD:
        try:
            upload_to_google_drive(out_path)
            drive_status = "OK"
        except Exception as e:
            drive_status = f"FAILED ({e})"

    # ── Summary ──────────────────────────────────────────────────────
    print(f"""
ENRICHMENT SUMMARY — {snapshot_date}
Companies in snapshot:        {total_companies}
Unique companies processed:   {len(companies)}
Skipped (size > 100):         {stats['size_filtered']}
Companies with domain found:  {stats['domain_found']}
Companies — no domain:        {stats['no_domain']}
Contacts after verification:  {stats['contacts_verified']}
LinkedIn URLs found:          {stats['linkedin_found']}
Output file:                  {out_path.name}
Drive upload:                 {drive_status}
""")

    return out_path


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Enrich weekly job snapshot via Hunter API")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit to first N unique companies (test mode)")
    args = parser.parse_args()
    run_enrichment(max_companies=args.limit)
