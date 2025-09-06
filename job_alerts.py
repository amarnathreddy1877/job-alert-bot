#!/usr/bin/env python3
"""
job_alerts.py
-------------------------------------------------------------------------------
Entry/Mid-Level Data Analyst Job Alerts (US + Remote-US)

What this script does:
- Pulls job listings directly from public ATS APIs (Greenhouse, Lever, SmartRecruiters)
- Filters titles to analyst-focused roles and excludes senior/manager/engineer/etc.
- Keeps only US/Remote-US roles
- Deduplicates using a lightweight JSON cache (so you only get NEW roles per run)
- Sends a clean, grouped HTML email via SendGrid
- Designed for GitHub Actions (runs hourly via a workflow)

Why ATS APIs?
- Stable, structured JSON (no fragile HTML scraping or headless browsers)
- Faster, cheaper, and ToS-friendly compared to scraping big job boards

Required GitHub Actions secrets:
  - SENDGRID_API_KEY   -> from your SendGrid account
  - SENDER_EMAIL       -> the verified sender (same email you verified in SendGrid)
  - RECIPIENT_EMAIL    -> where to send the alerts (your email)
"""

from __future__ import annotations

import os
import re
import json
import time
import datetime as dt
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode

import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# File with your company list (see README for format). If missing, we use a
# conservative built-in default so the repo still "works out of the box".
COMPANIES_FILE = "companies.json"

# Cache of already-emailed jobs (so we only email new ones).
CACHE_FILE = ".cache/seen_ids.json"

# Analyst-focused keep list (tight on purpose).
KEYWORDS_POSITIVE_TITLES = [
    "data analyst",
    "business analyst",
    "reporting analyst",
    "analytics analyst",
    "bi analyst",
    "insights analyst",
    "business intelligence analyst",
    "product data analyst",
    "operations analyst",
    "fraud analyst",
    "risk analyst",
    "people data analyst",
]

# Hard blocks so engineers/designers/scientists don't slip in.
KEYWORDS_NEGATIVE_TITLES = [
    "engineer", "developer", "devops", "sre",
    "scientist", "architect", "designer", "product designer",
    "mobile", "ios", "android",
    "principal", "staff", "senior", "sr.", "lead", "manager", "mgr",
    "director", "head", "vp", "svp", "evp"
]

# Light US/Remote-US location gate. We accept common markers like "Remote - US".
US_LOCATION_PAT = re.compile(
    r"\b(United States|USA|U\.S\.|US|Remote\s*-\s*US|Remote\s*US|United States of America|"
    r"AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b",
    re.I
)

# Simple HTTP settings
TIMEOUT = 30
RETRIES = 2
BACKOFF_SECONDS = 1.5


# -----------------------------------------------------------------------------
# Utility helpers
# -----------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Lowercase and collapse whitespace for reliable matching."""
    return re.sub(r"\s+", " ", (text or "")).strip().lower()


def _title_is_analyst(title: str) -> bool:
    """
    Keep only analystish titles while rejecting senior/lead/engineer/etc.
    """
    t = _normalize(title)
    if any(bad in t for bad in KEYWORDS_NEGATIVE_TITLES):
        return False
    return any(good in t for good in KEYWORDS_POSITIVE_TITLES)


def _is_us(loc: str) -> bool:
    """Rudimentary US/Remote-US filter."""
    return bool(US_LOCATION_PAT.search(loc or ""))


def _http_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    GET JSON with small retry/backoff. Keeps logs simple for GitHub Actions.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(RETRIES):
        try:
            full = f"{url}?{urlencode(params)}" if params else url
            r = requests.get(
                full,
                timeout=TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 (JobAlertsBot)"},
            )
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last_exc = exc
            if attempt < RETRIES - 1:
                time.sleep(BACKOFF_SECONDS * (attempt + 1))
    # If we reach here, give up and raise the last exception.
    raise last_exc  # type: ignore[misc]


def _load_cache() -> Dict[str, float]:
    """Load dedup cache from disk. Corruption returns an empty cache."""
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}


def _save_cache(cache: Dict[str, float]) -> None:
    """Persist dedup cache."""
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2)


def _job_key(company: str, job_id: str) -> str:
    """Stable composite key for deduping."""
    return f"{company}::{job_id}"


def _pretty_subject(total: int) -> str:
    if total == 0:
        return "No New Data Analyst Jobs"
    return f"{total} New Data Analyst Job{'s' if total != 1 else ''}"


def _load_companies() -> List[Dict[str, Any]]:
    """
    Read companies from companies.json.
    Fallback: a small Greenhouse set so the project works if the file is missing.
    """
    default_companies = [
        {"name": "Stripe",    "type": "greenhouse", "board": "stripe"},
        {"name": "Affirm",    "type": "greenhouse", "board": "affirm"},
        {"name": "Robinhood", "type": "greenhouse", "board": "robinhood"},
        {"name": "Dropbox",   "type": "greenhouse", "board": "dropbox"},
        {"name": "Figma",     "type": "greenhouse", "board": "figma"},
        {"name": "Instacart", "type": "greenhouse", "board": "instacart"},
        {"name": "Chime",     "type": "greenhouse", "board": "chime"},
    ]
    try:
        with open(COMPANIES_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        print(f"[info] {COMPANIES_FILE} not found; using built-in defaults.")
        return default_companies
    except Exception as exc:
        print(f"[warn] failed to parse {COMPANIES_FILE}: {exc}; using defaults.")
        return default_companies


# -----------------------------------------------------------------------------
# Fetchers (ATS APIs)
# -----------------------------------------------------------------------------

def fetch_greenhouse(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Greenhouse: https://boards-api.greenhouse.io/v1/boards/<board>/jobs?content=true
    """
    board = company["board"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
    data = _http_json(url, params={"content": "true"})
    out: List[Dict[str, Any]] = []

    for j in data.get("jobs", []):
        title = j.get("title", "")
        if not _title_is_analyst(title):
            continue

        loc = (j.get("location") or {}).get("name", "") or ""
        if not loc:
            # Some postings include 'offices' as a fallback
            offices = (j.get("offices") or [])
            if offices:
                loc = offices[0].get("name", "")

        if not _is_us(loc):
            # Some Greenhouse postings put "Remote US" in description/content.
            desc = _normalize(j.get("content", ""))
            if not ("remote" in desc and "us" in desc):
                continue

        out.append({
            "id": str(j.get("id")),
            "title": title,
            "location": loc,
            "link": j.get("absolute_url") or "",
        })
    return out


def fetch_lever(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Lever: https://api.lever.co/v0/postings/<site>?mode=json
    """
    site = company["site"]
    url = f"https://api.lever.co/v0/postings/{site}"
    data = _http_json(url, params={"mode": "json"})
    out: List[Dict[str, Any]] = []

    for j in data:
        title = j.get("text", "")
        if not _title_is_analyst(title):
            continue

        loc = j.get("categories", {}).get("location") or ""
        if not _is_us(loc):
            # Lever sometimes encodes Remote-US in different fields; keep it simple.
            if "remote" not in _normalize(loc):
                continue

        out.append({
            "id": j.get("id") or j.get("lever_id") or j.get("hostedUrl", ""),
            "title": title,
            "location": loc,
            "link": j.get("hostedUrl") or j.get("applyUrl") or "",
        })
    return out


def fetch_smartrecruiters(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    SmartRecruiters (paginated):
      https://api.smartrecruiters.com/v1/companies/<company>/postings?limit=100&offset=0
    """
    comp = company["company"]
    url = f"https://api.smartrecruiters.com/v1/companies/{comp}/postings"
    out: List[Dict[str, Any]] = []

    for page in range(0, 3):  # up to 300 latest roles
        data = _http_json(url, params={"limit": 100, "offset": page * 100})
        for j in data.get("content", []):
            title = j.get("name", "")
            if not _title_is_analyst(title):
                continue

            loc_parts = j.get("location") or {}
            loc = " ".join([
                str(loc_parts.get("country") or "").strip(),
                str(loc_parts.get("region") or "").strip(),
                str(loc_parts.get("city") or "").strip(),
            ]).strip()

            if not _is_us(loc):
                if "remote" not in _normalize(loc):
                    continue

            # Build a robust link (prefer applyUrl; fallback to a canonical pattern)
            link = j.get("applyUrl") or ""
            if not link:
                jid = str(j.get("id"))
                link = f"https://careers.smartrecruiters.com/{comp}/{jid}"

            out.append({
                "id": str(j.get("id")),
                "title": title,
                "location": loc,
                "link": link,
            })

        if data.get("last", True):
            break

    return out


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "smartrecruiters": fetch_smartrecruiters,
}


# -----------------------------------------------------------------------------
# Email formatting + sending
# -----------------------------------------------------------------------------

def _build_email_payload(new_by_company: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    """Return minimal dict with subject/html for SendGrid."""
    now = dt.datetime.now().astimezone()
    total = sum(len(v) for v in new_by_company.values())
    subject = f"[{now.strftime('%-I %p')}] {_pretty_subject(total)}"

    if total == 0:
        html = """
        <h2>No new analyst roles this hour</h2>
        <p>I’ll keep checking hourly. Consider adding more companies in <code>companies.json</code>.</p>
        """
        return {"subject": subject, "html": html}

    parts: List[str] = [f"<h2 style='margin:0'>{_pretty_subject(total)}</h2>"]
    for company, jobs in sorted(new_by_company.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        if not jobs:
            continue
        parts.append(f"<h3 style='margin-top:20px'>{company}</h3><ul>")
        for j in jobs:
            title = j.get("title") or "Untitled role"
            link = j.get("link") or "#"
            loc = f" – {j.get('location','')}" if j.get("location") else ""
            parts.append(f"<li><a href='{link}' target='_blank' rel='noopener'>{title}</a>{loc}</li>")
        parts.append("</ul>")
    parts.append("<p style='color:#666;font-size:12px;margin-top:16px'>Source: public ATS APIs (Greenhouse/Lever/SmartRecruiters). Filters: US + entry/mid-level analyst only.</p>")

    return {"subject": subject, "html": "\n".join(parts)}


def _send_email(payload: Dict[str, str]) -> None:
    """Send the email via SendGrid. Fail fast with a helpful error."""
    api_key = os.environ.get("SENDGRID_API_KEY")
    sender = os.environ.get("SENDER_EMAIL")
    recipient = os.environ.get("RECIPIENT_EMAIL")

    missing = [k for k, v in {
        "SENDGRID_API_KEY": api_key,
        "SENDER_EMAIL": sender,
        "RECIPIENT_EMAIL": recipient,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env var(s): {', '.join(missing)}")

    sg = SendGridAPIClient(api_key)
    msg = Mail(from_email=sender, to_emails=recipient,
               subject=payload["subject"], html_content=payload["html"])
    resp = sg.send(msg)
    print("SendGrid status:", resp.status_code)
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid error {resp.status_code}: {getattr(resp, 'body', '')}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> None:
    companies = _load_companies()
    seen = _load_cache()

    new_by_company: Dict[str, List[Dict[str, Any]]] = {}
    total_new = 0

    for c in companies:
        name = c.get("name", "Unknown")
        kind = c.get("type")
        fetcher = FETCHERS.get(kind)

        if not fetcher:
            print(f"[skip] {name}: unknown type '{kind}'")
            continue

        try:
            jobs = fetcher(c)
        except Exception as exc:
            print(f"[warn] {name}: {exc}")
            jobs = []

        fresh: List[Dict[str, Any]] = []
        for j in jobs:
            # Compose a stable ID for deduping (prefer provider job ID, fallback to link)
            jid = str(j.get("id") or j.get("link") or j.get("title"))
            key = _job_key(name, jid)
            if key in seen:
                continue
            fresh.append(j)
            seen[key] = time.time()

        if fresh:
            total_new += len(fresh)
        new_by_company[name] = fresh

    payload = _build_email_payload(new_by_company)
    _send_email(payload)

    # Keep cache tidy: keep only last 30 days
    cutoff = time.time() - 30 * 24 * 3600
    seen = {k: v for k, v in seen.items() if v >= cutoff}
    _save_cache(seen)

    print(f"Done. New jobs this run: {total_new}")


if __name__ == "__main__":
    main()
