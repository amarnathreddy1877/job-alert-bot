import os
import re
import json
import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# ---------------------------------------------------------------------------
# Load company URLs
# ---------------------------------------------------------------------------

def load_companies() -> List[Dict]:
    with open("companies.json", "r", encoding="utf-8") as fh:
        return json.load(fh)

# ---------------------------------------------------------------------------
# Keyword-based matching logic
# ---------------------------------------------------------------------------

def is_data_analyst_job(title: str, desc: str = "") -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    desc = desc.lower()

    job_keywords = [
        "analyst", "analytics", "intelligence", "insights",
        "data science", "data engineer", "reporting", "statistics"
    ]

    tech_keywords = [
        "sql", "tableau", "power bi", "excel", "python",
        "r language", "dashboard", "data pipeline"
    ]

    title_match = any(k in title for k in job_keywords)
    desc_match = any(k in desc for k in tech_keywords)

    return title_match or desc_match

# ---------------------------------------------------------------------------
# Scraper logic (simple HTML scraping)
# ---------------------------------------------------------------------------

def scrape_jobs(companies: List[Dict]) -> List[Dict]:
    jobs = []

    for company in companies:
        url = company["url"]
        name = company["name"]
        print(f"🔍 Scraping jobs for: {name} — {url}")

        try:
            res = requests.get(url, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            links = soup.find_all("a", href=True)
            print(f"[{name}] Found {len(links)} total links")

            matched = 0
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
                            "link": job_url,
                            "company": name,
                            "location": company.get("location", "")
                        })
                        matched += 1
                except Exception as e:
                    continue

            print(f"[{name}] Found {matched} matched jobs\n")
        except Exception as e:
            print(f"[{name}] Failed to scrape due to: {e}")
            continue

    return jobs

# ---------------------------------------------------------------------------
# Group jobs by company
# ---------------------------------------------------------------------------

def group_jobs(jobs: List[Dict]) -> Dict[str, List[Dict]]:
    grouped = {}
    for job in jobs:
        grouped.setdefault(job["company"], []).append(job)
    return grouped

# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------

SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]

def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs Update"

    if not any(jobs_by_company.values()):
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    sections = []
    for company, jobs in sorted(jobs_by_company.items()):
        if not jobs:
            continue
        sections.append(f"<h3>{company}</h3><ul>")
        for job in jobs:
            loc = f" – {job['location']}" if job['location'] else ""
            sections.append(f"<li><a href='{job['link']}'>{job['title']}</a>{loc}</li>")
        sections.append("</ul>")
    html = "\n".join(sections)
    return {"subject": subject, "html": html}

def send_email(email: Dict[str, str]) -> None:
    message = Mail(
        from_email=os.environ["SENDER_EMAIL"],
        to_emails=os.environ["RECIPIENT_EMAIL"],
        subject=email["subject"],
        html_content=email["html"],
    )

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print("SendGrid status:", response.status_code)
    if response.status_code not in (200, 202):
        print("Error body:", response.body)
        raise RuntimeError(f"SendGrid error {response.status_code}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)
    jobs_by_company = group_jobs(jobs)
    print(f"📝 Summary: {sum(len(j) for j in jobs_by_company.values())} jobs matched across {len(jobs_by_company)} companies.\n")
    email = build_email(jobs_by_company)
    send_email(email)

if __name__ == "__main__":
    main()
