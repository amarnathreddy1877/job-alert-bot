import os
import re
import json
import datetime as dt
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from mailjet_rest import Client

"""
job_alerts.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Scrapes career pages of H‑1B‑friendly companies for entry‑ to mid‑level
Data‑Analytics roles located in the United States and emails an hourly digest
via Mailjet. Designed for deployment as a Render Cron Job (UTC‑based schedule).

Environment variables required on Render:
  MJ_API_KEY       – Mailjet API Key
  MJ_API_SECRET    – Mailjet Secret Key
  SENDER_EMAIL     – From address (eg. alerts@yourdomain.com)
  RECIPIENT_EMAIL  – "amarnathreddymalkireddy@gmail.com"

Files expected in the same directory:
  companies.json   – List of companies and their career‑page URLs (see template
                     at bottom of this file).

Add or remove companies by editing companies.json – no code changes needed.
"""

# ---------------------------------------------------------------------------
# Configuration -------------------------------------------------------------
# ---------------------------------------------------------------------------

MJ_API_KEY = os.environ["MJ_API_KEY"]
MJ_API_SECRET = os.environ["MJ_API_SECRET"]

SENDER = os.environ.get("SENDER_EMAIL", "alerts@example.com")
RECIPIENT = os.environ.get("RECIPIENT_EMAIL", "amarnathreddymalkireddy@gmail.com")

# Keywords that qualify a job as data‑analytics *entry/mid‑level* (tweak as needed)
KEYWORDS = [
    "data analyst", "business analyst", "analytics", "bi analyst",
    "reporting analyst", "insights analyst"
]

US_LOCATION_PATTERN = re.compile(
    r"\b(United States|USA|U\.S\.|AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|"
    r"KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|"
    r"TX|UT|VT|VA|WA|WV|WI|WY)\b", re.I
)

COMPANIES_FILE = os.environ.get("COMPANIES_FILE", "companies.json")


# ---------------------------------------------------------------------------
# Helper Functions ----------------------------------------------------------
# ---------------------------------------------------------------------------

def load_companies() -> List[Dict]:
    """Read companies.json and return as list of dicts."""
    with open(COMPANIES_FILE, "r", encoding="utf-8") as fh:
        return json.load(fh)


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def is_analytics_role(title: str) -> bool:
    t = normalize(title)
    return any(kw in t for kw in KEYWORDS) and "senior" not in t and "manager" not in t


def in_usa(location: str) -> bool:
    return bool(US_LOCATION_PATTERN.search(location))


def fetch_generic(company: Dict) -> List[Dict]:
    """Fallback scraper for career pages rendered server‑side (may miss JS pages)."""
    jobs = []
    resp = requests.get(company["url"], timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    for a in soup.find_all("a", href=True):
        title = a.get_text(" ", strip=True)
        if not title or not is_analytics_role(title):
            continue
        link = requests.compat.urljoin(company["url"], a["href"])
        loc_tag = a.find_next(string=re.compile("[A-Za-z]{2}\s*,?\s*\w{2}", re.I))
        loc = loc_tag.strip() if loc_tag else ""
        if loc and not in_usa(loc):
            continue
        jobs.append({"title": title, "location": loc, "link": link})
    return jobs


def fetch_amazon(_: Dict) -> List[Dict]:
    url = (
        "https://www.amazon.jobs/en/search.json?"
        "base_query=&category=analytics%20%26%20insights&country=USA&size=50"
    )
    data = requests.get(url, timeout=20).json()
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
    url = "https://rds.google.com/research/roles/list?hl=en_US&jlo=en_US&src=SERP"
    data = requests.get(url, timeout=20).json()
    jobs = []
    for job in data.get("jobs", []):
        title = job["title"]
        loc = ", ".join(job["location"]["display_location"].split(",")[:2])
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


def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs (H1B‑Friendly)"
    sections = []
    for comp, jobs in sorted(jobs_by_company.items()):
        if not jobs:
            continue
        sections.append(f"<h3>{comp}</h3><ul>")
        for j in jobs:
            loc = f" – {j['location']}" if j['location'] else ""
            sections.append(f"<li><a href='{j['link']}'>{j['title']}</a>{loc}</li>")
        sections.append("</ul>")
    html_body = "\n".join(sections) if sections else "<p>No new jobs found this hour.</p>"
    return {"subject": subject, "html": html_body}


def send_email(email: Dict) -> None:
    mailjet = Client(auth=(MJ_API_KEY, MJ_API_SECRET), version="v3.1")
    data = {
        "Messages": [
            {
                "From": {"Email": SENDER, "Name": "Job Alerts Bot"},
                "To": [{"Email": RECIPIENT, "Name": "You"}],
                "Subject": email["subject"],
                "HTMLPart": email["html"],
            }
        ]
    }
    result = mailjet.send.create(data=data)
    if result.status_code not in (200, 201):
        raise RuntimeError(f"Mailjet error: {result.status_code} {result.json()}")
    print("Email sent:", result.status_code)


def main() -> None:
    companies = load_companies()
    jobs_by_comp = {c["name"]: get_jobs(c) for c in companies}
    email = build_email(jobs_by_comp)
    send_email(email)


if __name__ == "__main__":
    main()

""" ---------------------------------------------------------------------
companies.json TEMPLATE (create this file alongside job_alerts.py):

[
  {"slug": "amazon", "name": "Amazon", "url": "https://www.amazon.jobs/en/search?category=analytics%20%26%20insights&country=USA"},
  {"slug": "google", "name": "Google", "url": "https://careers.google.com/jobs/results/?distance=50&hl=en_US&employment_type=FULL_TIME&location=United%20States"},
  {"slug": "microsoft", "name": "Microsoft", "url": "https://careers.microsoft.com/us/en/search-results?keywords=analyst"}
]

Add more objects as needed – just ensure each has unique "slug" (lowercase, no spaces).
"""
