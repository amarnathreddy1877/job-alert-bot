import os
import re
import json
import datetime as dt
from typing import List, Dict
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from bs4 import BeautifulSoup
import requests

# ---------------------------------------------------------------------------
# Load companies list from JSON
# ---------------------------------------------------------------------------
def load_companies(path: str = "companies.json") -> List[Dict[str, str]]:
    with open(path, "r") as fh:
        return json.load(fh)

# ---------------------------------------------------------------------------
# Keyword matcher
# ---------------------------------------------------------------------------

def is_data_analyst_job(title: str, description: str) -> bool:
    title = re.sub("\s+", " ", title).strip().lower()
    description = description.lower()
    return "analyst" in title or "data analyst" in description

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

def scrape_jobs(companies: List[Dict[str, str]]) -> List[Dict]:
    jobs = []
    for company in companies:
        url = company["url"]
        try:
            res = requests.get(url, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            links = soup.find_all("a", href=True)
            for a in links:
                job_title = a.get_text(strip=True)
                href = a["href"]
                job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href

                try:
                    job_res = requests.get(job_url, timeout=10)
                    job_soup = BeautifulSoup(job_res.text, "html.parser")
                    job_desc = job_soup.get_text()
                    if is_data_analyst_job(job_title, job_desc):
                        jobs.append({
                            "title": job_title,
                            "url": job_url,
                            "company": company["name"],
                            "location": company.get("location", "")
                        })
                except:
                    continue
        except:
            continue
    return jobs

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
        html = "<p>No data analyst jobs were found in this hour’s scan. We’ll keep checking hourly!</p>"
        return {"subject": subject, "html": html}

    sections: List[str] = []
    for comp, jobs in sorted(jobs_by_company.items()):
        if not jobs:
            continue
        sections.append(f"<h3>{comp}</h3><ul>")
        for j in jobs:
            loc = f" – {j['location']}" if j['location'] else ""
            sections.append(f"<li><a href='{j['url']}'>{j['title']}</a>{loc}</li>")
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
        print("SendGrid error:", response.body)
        raise RuntimeError(f"SendGrid error {response.status_code}: {response.body}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)

    jobs_by_company: Dict[str, List[Dict]] = {}
    for job in jobs:
        comp = job["company"]
        jobs_by_company.setdefault(comp, []).append(job)

    email = build_email(jobs_by_company)
    send_email(email)

if __name__ == "__main__":
    main()
