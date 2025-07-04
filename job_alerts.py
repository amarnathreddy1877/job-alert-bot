import os
import re
import json
import datetime as dt
from typing import List, Dict

import requests
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

"""job_alerts.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Hourly job-alert bot for entry-/mid-level **Data-Analytics** roles that sponsor
H-1B in the USA.

• Scrapes each career-site URL listed in `companies.json`
• Filters titles by keywords, removes senior/manager roles
• Sends a grouped HTML digest via **SendGrid**
• Designed to run inside GitHub Actions (`.github/workflows/job-alert.yml`)

Environment variables required by the workflow:
  SENDGRID_API_KEY   – from SendGrid dashboard
  SENDER_EMAIL       – verified sender (the same Gmail you registered)
  RECIPIENT_EMAIL    – where you want to receive the alerts (your Gmail)

All other settings (keywords, hours, company list) live in this repo and can be
changed without touching the GitHub Action configuration.
"""

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

KEYWORDS = [
    "data analyst", "business analyst", "analytics", "bi analyst",
    "reporting analyst", "insights analyst"
]

US_LOCATION_PATTERN = re.compile(
    r"\b(United States|USA|U\.S\.|AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|"  # noqa: E501
    r"KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|"  # noqa: E501
    r"TX|UT|VT|VA|WA|WV|WI|WY)\b", re.I
)

COMPANIES_FILE = "companies.json"


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def load_companies() -> List[Dict]:
    """Load the JSON list of companies to scrape."""
    with open(COMPANIES_FILE, "r", encoding="utf-8") as fh:
        return json.load(fh)


def normalize(text: str) -> str:
    """Lower-case and collapse whitespace for easier keyword matching."""
    return re.sub(r"\s+", " ", text).strip().lower()


def is_analytics_role(title: str) -> bool:
    t = normalize(title)
    return any(kw in t for kw in KEYWORDS) and "senior" not in t and "manager" not in t


def in_usa(location: str) -> bool:
    return bool(US_LOCATION_PATTERN.search(location))


# ---------------------------------------------------------------------------
# Scrapers (generic + company-specific)
# ---------------------------------------------------------------------------

def fetch_generic(company: Dict) -> List[Dict]:
    """Very simple scraper for career pages rendered server-side."""
    jobs: List[Dict] = []
    resp = requests.get(company["url"], timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for link in soup.find_all("a", href=True):
        title = link.get_text(" ", strip=True)
        if not title or not is_analytics_role(title):
            continue
        href = requests.compat.urljoin(company["url"], link["href"])
        loc_tag = link.find_next(string=re.compile(r"[A-Za-z]{2}\s*,?\s*\w{2}", re.I))
        loc = loc_tag.strip() if loc_tag else ""
        if loc and not in_usa(loc):
            continue
        jobs.append({"title": title, "location": loc, "link": href})
    return jobs


def fetch_amazon(_: Dict) -> List[Dict]:
    url = (
        "https://www.amazon.jobs/en/search.json?"
        "base_query=&category=analytics%20%26%20insights&country=USA&size=50"
    )
    data = requests.get(url, timeout=30).json()
    jobs = []
    for item in data.get("jobs", []):
        title = item["title"]
        if not is_analytics_role(title):
            continue
        jobs.append({
            "title": title,
            "location": item.get("location"),
            "link": f"https://www.amazon.jobs/en/jobs/{item['id']}"
        })
    return jobs


def fetch_google(_: Dict) -> List[Dict]:
    # Google careers JSON endpoint periodically changes; this is a best-effort.
    api = "https://rds.google.com/research/roles/list?hl=en_US&jlo=en_US&src=SERP"
    try:
        data = requests.get(api, timeout=30).json()
    except Exception:
        return []
    jobs = []
    for job in data.get("jobs", []):
        title = job["title"]
        loc = job.get("location", {}).get("display_location", "")
        loc = ", ".join(loc.split(",")[:2])
        if not is_analytics_role(title) or not in_usa(loc):
            continue
        jobs.append({
            "title": title,
            "location": loc,
            "link": "https://careers.google.com/jobs/results/" + job["job_id"]
        })
    return jobs


FETCHERS = {
    "amazon": fetch_amazon,
    "google": fetch_google,
}


def get_jobs(company: Dict) -> List[Dict]:
    fn = FETCHERS.get(company["slug"], fetch_generic)
    try:
        return fn(company)
    except Exception as exc:
        print(f"[warn] {company['name']}: {exc}")
        return []


# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------

SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]


def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs (H1B-Friendly)"
    if not any(jobs_by_company.values()):
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    sections: List[str] = []
    for comp, jobs in sorted(jobs_by_company.items()):
        if not jobs:
            continue
        sections.append(f"<h3>{comp}</h3><ul>")
        for j in jobs:
            loc = f" – {j['location']}" if j['location'] else ""
            sections.append(f"<li><a href='{j['link']}'>{j['title']}</a>{loc}</li>")
        sections.append("</ul>")
    html = "\n".join(sections)
    return {"subject": subject, "html": html}


def send_email(email: Dict[str, str]) -> None:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    message = Mail(
        from_email=os.environ.get("SENDER_EMAIL"),
        to_emails=os.environ.get("RECIPIENT_EMAIL"),
        subject=email["subject"],
        html_content=email["html"],
    )
    response = sg.send(message)
    print("SendGrid status:", response.status_code)
    if response.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid error {response.status_code}: {response.body}")


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    companies = load_companies()
    jobs_by_comp = {c["name"]: get_jobs(c) for c in companies}
    email = build_email(jobs_by_comp)
    send_email(email)


if __name__ == "__main__":
    main()
