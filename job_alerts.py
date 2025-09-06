import os
import re
import json
import requests
import datetime as dt
from typing import List, Dict
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# ---------------------------------------------------------------------------
# Load companies from companies.json
# ---------------------------------------------------------------------------

def load_companies(filepath: str) -> List[Dict]:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Job Scraper
# ---------------------------------------------------------------------------

def scrape_jobs(companies: List[Dict]) -> List[Dict]:
    jobs = []
    for company in companies:
        name = company["name"]
        url = company["url"]
        print(f"🔍 Scraping jobs for: {name} — {url}")
        try:
            res = requests.get(url, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")

            anchors = soup.find_all("a", href=True)
            total_links = len(anchors)
            print(f"[{name}] Found {total_links} total links")

            matched = 0
            for a in anchors:
                title = a.get_text(strip=True)
                title_clean = re.sub(r"\s+", " ", title).strip().lower()
                href = a["href"]
                job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href

                # --- SMART LINK FILTER ---
                if name.lower() == "google" and not re.search(r"/jobs/results/\d+", href):
                    continue
                if name.lower() == "amazon" and not re.search(r"/job/", href):
                    continue

                # --- FLEXIBLE MATCH ---
                if "analyst" in title_clean and len(title_clean.split()) < 12:
                    jobs.append({
                        "title": title.title(),
                        "url": job_url,
                        "company": name
                    })
                    matched += 1

            print(f"[{name}] Found {matched} matched jobs")
        except Exception as e:
            print(f"[{name}] Error scraping: {e}")
            continue
    return jobs


# ---------------------------------------------------------------------------
# Organize by company
# ---------------------------------------------------------------------------

def group_jobs_by_company(jobs: List[Dict]) -> Dict[str, List[Dict]]:
    grouped = {}
    for job in jobs:
        grouped.setdefault(job["company"], []).append(job)
    return grouped

# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------

SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]

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
        for j in sorted(jobs, key=lambda x: x["title"]):
            sections.append(f"<li><a href='{j['url']}'>{j['title']}</a></li>")
        sections.append("</ul>")
    html = "\n".join(sections)
    return {"subject": subject, "html": html}

def send_email(email: Dict[str, str]) -> None:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=RECIPIENT_EMAIL,
        subject=email["subject"],
        html_content=email["html"],
    )
    response = sg.send(message)
    print("SendGrid status:", response.status_code)
    if response.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid error {response.status_code}: {response.body}")

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    companies = load_companies("companies.json")
    jobs = scrape_jobs(companies)
    jobs_by_company = group_jobs_by_company(jobs)
    email = build_email(jobs_by_company)
    send_email(email)
