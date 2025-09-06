import os
import re
import json
import datetime as dt
import requests
from typing import List, Dict
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -------------------------
# Config and Environment
# -------------------------
SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]

# -------------------------
# Keyword Matching Logic
# -------------------------
def is_data_analyst_job(title: str, desc: str = "") -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    keywords = ["analyst", "analytics", "insights", "business intelligence", "data science"]
    return any(k in title for k in keywords)

# -------------------------
# Scraping Function
# -------------------------
def scrape_jobs(companies: List[Dict[str, str]]) -> List[Dict]:
    jobs = []
    for company in companies:
        name = company["name"]
        url = company["url"]
        location = company.get("location", "")

        print(f"\n🔍 Scraping jobs for: {name} — {url}")
        try:
            res = requests.get(url, timeout=20)
            soup = BeautifulSoup(res.text, "html.parser")

            links = soup.find_all("a", href=True)
            print(f"[{name}] Found {len(links)} total links")

            match_count = 0
            for a in links:
                job_title = a.get_text(strip=True)
                if not job_title or len(job_title) < 4:
                    continue
                href = a["href"]
                job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href

                # Optional: Fetch individual job page to check description
                try:
                    job_res = requests.get(job_url, timeout=10)
                    job_desc = job_res.text.lower()
                except:
                    job_desc = ""

                if is_data_analyst_job(job_title, job_desc):
                    jobs.append({
                        "company": name,
                        "title": job_title,
                        "link": job_url,
                        "location": location
                    })
                    match_count += 1
            print(f"[{name}] Found {match_count} matched jobs")

        except Exception as e:
            print(f"[{name}] Error: {e}")
            continue
    return jobs

# -------------------------
# Group by Company
# -------------------------
def group_jobs_by_company(jobs: List[Dict]) -> Dict[str, List[Dict]]:
    grouped = {}
    for job in jobs:
        grouped.setdefault(job["company"], []).append(job)
    return grouped

# -------------------------
# Build Email
# -------------------------
def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs"

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

# -------------------------
# Send Email via SendGrid
# -------------------------
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

# -------------------------
# Load Company URLs
# -------------------------
def load_companies() -> List[Dict[str, str]]:
    with open("companies.json", "r", encoding="utf-8") as fh:
        return json.load(fh)

# -------------------------
# Main
# -------------------------
def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)
    jobs_by_company = group_jobs_by_company(jobs)
    print(f"\n📝 Summary: {sum(len(j) for j in jobs_by_company.values())} jobs matched across {len(jobs_by_company)} companies.\n")
    email = build_email(jobs_by_company)
    send_email(email)

if __name__ == "__main__":
    main()
