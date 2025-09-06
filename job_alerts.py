# job_alerts.py
import os
import re
import json
import time
import math
import datetime as dt
from typing import Dict, List, Any, Optional
from urllib.parse import urlencode

import requests
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

"""
Job Alerts Bot — Entry/Mid Data Analyst roles (US only)

- Scrapes company career pages via their ATS public APIs:
  * Greenhouse
  * Lever
  * SmartRecruiters
- Filters titles for data/analytics keywords, excludes senior/manager/director
- Filters for US-based roles (remote-US included)
- Caches seen job IDs so you only get *new* jobs
- Sends HTML digest via SendGrid
- Intended to run hourly in GitHub Actions

Required secrets (set in repo Settings ▸ Secrets and variables ▸ Actions):
  SENDGRID_API_KEY, SENDER_EMAIL, RECIPIENT_EMAIL

Config files:
  companies.json   -> list of companies & their ATS types
  requirements.txt -> dependencies
"""

# ------------------------------ Settings ------------------------------------

COMPANIES_FILE = "companies.json"
CACHE_FILE = ".cache/seen_ids.json"

KEYWORDS_POSITIVE = [
    # core titles/phrases
    "data analyst", "business analyst", "reporting analyst",
    "analytics", "bi analyst", "insights analyst",
    "business intelligence", "product analyst", "operations analyst",
    # duties
    "dashboard", "kpi", "reporting", "visualization", "bi",
    # tech stack
    "sql", "python", "power bi", "tableau", "looker", "looker studio",
    "excel", "bigquery", "snowflake", "redshift", "dbt"
]

KEYWORDS_NEGATIVE = [
    "senior", "sr.", "principal", "lead", "staff",
    "manager", "mgr", "director", "head", "vp", "svp", "evp",
    "architect"
]

US_LOCATION_PAT = re.compile(
    r"\b(United States|USA|U\.S\.|US|Remote - US|Remote US|United States of America|"
    r"AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY)\b",
    re.I
)

TIMEOUT = 30
RETRIES = 2
BACKOFF = 1.5


# ---------------------------- Utility helpers -------------------------------

def _load_companies() -> List[Dict[str, Any]]:
    with open(COMPANIES_FILE, "r", encoding="utf-8") as fh:
        return json.load(fh)

def _normalize(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip().lower()

def _is_entry_mid_title(title: str) -> bool:
    t = _normalize(title)
    if any(bad in t for bad in KEYWORDS_NEGATIVE):
        return False
    return any(good in t for good in KEYWORDS_POSITIVE)

def _is_us(loc: str) -> bool:
    return bool(US_LOCATION_PAT.search(loc or ""))

def _http_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    last_exc = None
    for attempt in range(RETRIES):
        try:
            full = f"{url}?{urlencode(params)}" if params else url
            r = requests.get(full, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            if attempt < RETRIES - 1:
                time.sleep(BACKOFF * (attempt + 1))
    raise last_exc

def _load_cache() -> Dict[str, float]:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

def _save_cache(cache: Dict[str, float]) -> None:
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2)

def _job_key(company: str, job_id: str) -> str:
    return f"{company}::{job_id}"

def _pretty_subject_count(total: int) -> str:
    if total == 0:
        return "No New Data Analyst Jobs"
    return f"{total} New Data Analyst Job{'s' if total != 1 else ''}"


# ------------------------------- Fetchers -----------------------------------

def fetch_greenhouse(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Example: https://boards-api.greenhouse.io/v1/boards/stripe/jobs?content=true
    board = company["board"]  # required
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"
    data = _http_json(url, params={"content": "true"})
    out: List[Dict[str, Any]] = []
    for j in data.get("jobs", []):
        title = j.get("title", "")
        if not _is_entry_mid_title(title):
            continue
        # locations: either j["location"]["name"] or offices list in content
        loc = (j.get("location") or {}).get("name", "") or ""
        # fallback: try first office
        if not loc:
            offices = (j.get("offices") or [])
            if offices:
                loc = offices[0].get("name", "")
        if not _is_us(loc):
            # allow remote roles with US in description
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
    # Example: https://api.lever.co/v0/postings/notion?mode=json
    site = company["site"]
    url = f"https://api.lever.co/v0/postings/{site}"
    data = _http_json(url, params={"mode": "json"})
    out: List[Dict[str, Any]] = []
    for j in data:
        title = j.get("text", "")
        if not _is_entry_mid_title(title):
            continue
        # locations: list of dicts with 'name'
        locs = j.get("categories", {}).get("location") or ""
        loc = locs if isinstance(locs, str) else ""
        if not _is_us(loc):
            # sometimes Lever includes "Remote (US)" in 'workType' or in 'location'
            if "remote" not in _normalize(loc):
                continue
        out.append({
            "id": j.get("id") or j.get("lever_id") or j.get("applyUrl", ""),
            "title": title,
            "location": loc,
            "link": j.get("hostedUrl") or j.get("applyUrl") or "",
        })
    return out

def fetch_smartrecruiters(company: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Example: https://api.smartrecruiters.com/v1/companies/snowflake/postings?limit=100
    comp = company["company"]
    url = f"https://api.smartrecruiters.com/v1/companies/{comp}/postings"
    # SmartRecruiters paginates; fetch first 3 pages (300 roles max)
    out: List[Dict[str, Any]] = []
    for page in range(1, 4):
        data = _http_json(url, params={"limit": 100, "offset": (page - 1) * 100})
        for j in data.get("content", []):
            title = j.get("name", "")
            if not _is_entry_mid_title(title):
                continue
            loc = (j.get("location") or {}).get("country", "") + " " + \
                  (j.get("location") or {}).get("region", "") + " " + \
                  (j.get("location") or {}).get("city", "")
            if not _is_us(loc):
                # allow remote-US markers in job.releaseStatus or type text
                if "remote" not in _normalize(loc):
                    continue
            out.append({
                "id": str(j.get("id")),
                "title": title,
                "location": loc.strip(),
                "link": j.get("ref", {}).get("jobAd", {}).get("sections", {}).get("companyDescription", "") or
                        j.get("applyUrl") or j.get("ref", {}).get("id"),
            })
        if data.get("last", True):
            break
    # Fix broken links: prefer 'applyUrl' when absolute
    for k in out:
        link = k.get("link", "")
        if link and not link.startswith("http"):
            # fallback: standard posting link pattern
            k["link"] = f"https://careers.smartrecruiters.com/{comp}/{k['id']}"
    return out


FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "smartrecruiters": fetch_smartrecruiters,
}


# -------------------------- Email & presentation ----------------------------

def _build_email_payload(new_by_company: Dict[str, List[Dict[str, Any]]]) -> Dict[str, str]:
    now = dt.datetime.now().astimezone()
    total = sum(len(v) for v in new_by_company.values())
    subject = f"[{now:%-I %p}] {_pretty_subject_count(total)}"

    if total == 0:
        html = """
        <h2>No new jobs this hour</h2>
        <p>I’ll keep checking hourly. Consider widening keywords or companies.json.</p>
        """
        return {"subject": subject, "html": html}

    parts = [f"<h2>{_pretty_subject_count(total)}</h2>"]
    for company, jobs in sorted(new_by_company.items(), key=lambda x: (-len(x[1]), x[0])):
        if not jobs:
            continue
        parts.append(f"<h3>{company}</h3><ul>")
        for j in jobs:
            loc = f" – {j.get('location','')}" if j.get("location") else ""
            link = j.get("link") or "#"
            title = j.get("title") or "Untitled role"
            parts.append(f"<li><a href='{link}' target='_blank' rel='noopener'>{title}</a>{loc}</li>")
        parts.append("</ul>")
    parts.append("<p style='color:#666;font-size:12px'>Source: public ATS APIs (Greenhouse/Lever/SmartRecruiters). Filters: US + entry/mid-level analyst keywords.</p>")
    return {"subject": subject, "html": "\n".join(parts)}

def _send_email(payload: Dict[str, str]) -> None:
    api_key = os.environ.get("SENDGRID_API_KEY")
    sender = os.environ.get("SENDER_EMAIL")
    recipient = os.environ.get("RECIPIENT_EMAIL")

    if not api_key or not sender or not recipient:
        raise RuntimeError("Missing SENDGRID_API_KEY and/or SENDER_EMAIL and/or RECIPIENT_EMAIL env vars.")

    sg = SendGridAPIClient(api_key)
    msg = Mail(from_email=sender, to_emails=recipient, subject=payload["subject"], html_content=payload["html"])
    resp = sg.send(msg)
    print("SendGrid status:", resp.status_code)
    if resp.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid error {resp.status_code}: {getattr(resp, 'body', '')}")


# --------------------------------- Main -------------------------------------

def main() -> None:
    companies = _load_companies()
    seen = _load_cache()

    new_by_company: Dict[str, List[Dict[str, Any]]] = {}
    total_new = 0

    for c in companies:
        name = c["name"]
        kind = c["type"]
        fn = FETCHERS.get(kind)
        if fn is None:
            print(f"[skip] {name}: unknown type '{kind}'")
            continue

        try:
            jobs = fn(c)
        except Exception as e:
            print(f"[warn] {name}: {e}")
            jobs = []

        fresh: List[Dict[str, Any]] = []
        for j in jobs:
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

    # Keep cache from growing unbounded: drop entries older than 30 days
    cutoff = time.time() - 30 * 24 * 3600
    seen = {k: v for k, v in seen.items() if v >= cutoff}
    _save_cache(seen)

    print(f"Done. New jobs this run: {total_new}")


if __name__ == "__main__":
    main()
