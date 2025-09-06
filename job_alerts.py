import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup
from typing import List, Dict
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import json


# ---------------------------------------------------------------------------
# Keywords Matcher (Any job title with "analyst")
# ---------------------------------------------------------------------------

def is_data_analyst_job(title: str, description: str) -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    return "analyst" in title


# ---------------------------------------------------------------------------
# Scraper Logic
# ---------------------------------------------------------------------------

def scrape_jobs(companies: List[Dict[str, str]]) -> List[Dict[str, str]]:
    jobs = []
    for company in companies:
        url = company["url"]
        name = company["name"]
        print(f"\n🔍 Scraping jobs for: {name} — {url}")

        try:
            res = requests.get(url, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")
            links = soup.find_all("a", href=True)
            print(f"[{name}] Found {len(links)} total links")

            for a in links:
                job_title = a.get_text(strip=True)
                href = a["href"]
                job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href

                try:
                    job_res = requests.get(job_url, timeout=10)
                    job_soup = BeautifulSoup(job_res.text, "html.parser")
                    job_desc = job_soup.get_text()

                    if is_data_analyst_job(job_title, job_desc):
                        print(f"✅ Matched Analyst Role: {job_title}")
                        jobs.append({
                            "title": job_title,
                            "url": job_url,
                            "company": name,
                            "location": company.get("location", "")
                        })
                except Exception as e:
                    print(f"  ⚠️ Skipping job detail fetch: {e}")
                    continue

        except Exception as e:
            print(f"[{name}] ❌ Error fetching URL: {e}")
            continue

    return jobs


# ---------------------------------------------------------------------------
# Group by Company
# ---------------------------------------------------------------------------

def group_jobs_by_company(jobs: List[Dict]) -> Dict[str, List[Dict]]:
    grouped = {}
    for job in jobs:
        grouped.setdefault(job["company"], []).append({
            "title": job["title"],
            "link": job["url"],
            "location": job.get("location", "")
        })
    return grouped


# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------

SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]

def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs (Broad Match)"

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
        print("❌ Email send failed:", response.body)


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with open("companies.json") as f:
        companies = json.load(f)

    jobs = scrape_jobs(companies)
    jobs_by_company = group_jobs_by_company(jobs)

    print(f"\n📝 Summary: {sum(len(v) for v in jobs_by_company.values())} jobs matched across {len(jobs_by_company)} companies.\n")
    
    email = build_email(jobs_by_company)
    send_email(email)
