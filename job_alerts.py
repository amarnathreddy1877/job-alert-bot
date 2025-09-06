import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# Load environment variables
SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]

# ---------------------------------------------------------------------------
# Utility: check if a job matches criteria
# ---------------------------------------------------------------------------
def is_data_analyst_job(title: str, desc: str) -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    desc = desc.lower()
    return "analyst" in title

# ---------------------------------------------------------------------------
# Scrape jobs from companies.json
# ---------------------------------------------------------------------------
def scrape_jobs(companies: List[Dict]) -> List[Dict]:
    jobs = []
    for company in companies:
        url = company["url"]
        print(f"Scraping: {company['name']} - {url}")
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
                        job_info = {
                            "title": job_title,
                            "url": job_url,
                            "company": company["name"],
                            "location": company.get("location", "")
                        }
                        print("Matched:", job_info)
                        jobs.append(job_info)

                except Exception as e:
                    print("Inner job page error:", e)
                    continue

        except Exception as e:
            print("Main company page error:", e)
            continue

    return jobs

# ---------------------------------------------------------------------------
# Email builder
# ---------------------------------------------------------------------------
def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs"

    if not any(jobs_by_company.values()):
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    sections: List[str] = []
    for company, jobs in jobs_by_company.items():
        if not jobs:
            continue
        sections.append(f"<h3>{company}</h3><ul>")
        for job in jobs:
            title = job.get("title", "Job Opening")
            url = job.get("url", "#")
            location = job.get("location", "")
            location_text = f" – {location}" if location else ""
            sections.append(f"<li><a href='{url}'>{title}</a>{location_text}</li>")
        sections.append("</ul>")

    html = "\n".join(sections)
    return {"subject": subject, "html": html}

# ---------------------------------------------------------------------------
# Email sender (SendGrid)
# ---------------------------------------------------------------------------
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
# Main
# ---------------------------------------------------------------------------
def main():
    import json
    with open("companies.json", "r") as f:
        companies = json.load(f)

    jobs = scrape_jobs(companies)
    jobs_by_company: Dict[str, List[Dict]] = {}
    for job in jobs:
        comp = job["company"]
        jobs_by_company.setdefault(comp, []).append(job)

    print("\nJobs grouped by company:")
    for company, jobs in jobs_by_company.items():
        print(f"- {company}: {len(jobs)} job(s)")
        for job in jobs:
            print(f"  → {job['title']} | {job['url']}")

    email = build_email(jobs_by_company)
    send_email(email)

if __name__ == "__main__":
    main()
