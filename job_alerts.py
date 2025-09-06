import os
import re
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")

# --- Job Matching Logic ---
KEYWORDS = [
    "sql", "python", "r", "tableau", "power bi", "looker",
    "excel", "data visualization", "statistics", "analytics", "dashboard", "reporting", "ETL"
]

# Flexible match: any job title with "analyst"
def is_data_analyst_job(title, description):
    if "analyst" in title.lower():
        return True
    match_count = sum(1 for kw in KEYWORDS if re.search(rf"\\b{kw}\\b", description.lower()))
    return match_count >= 2

# --- Scraper Function ---
def scrape_jobs(companies):
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

                # Fetch job page
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

# --- Email Sender ---
def send_email(subject, content):
    data = {
        "personalizations": [{"to": [{"email": TO_EMAIL}]}],
        "from": {"email": FROM_EMAIL},
        "subject": subject,
        "content": [{"type": "text/plain", "value": content}]
    }

    response = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json"
        },
        json=data
    )
    print("SendGrid status:", response.status_code)
    print("Response:", response.text)

# --- Load Companies ---
def load_companies():
    with open("companies.json", "r") as fh:
        return json.load(fh)

# --- Main Script ---
def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)

    if jobs:
        print(f"Found {len(jobs)} matching jobs")
        grouped = {}
        for job in jobs:
            grouped.setdefault(job["company"], []).append(job)

        message_lines = ["Here are the latest data analyst jobs:"]
        for company, postings in grouped.items():
            message_lines.append(f"\n{company}:")
            for job in postings:
                message_lines.append(f"- {job['title']}\n  {job['url']}")
        content = "\n".join(message_lines)
        subject = f"{len(jobs)} New Data Analyst Job(s) Found - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    else:
        content = "No data analyst jobs were found in this hour’s scan. We’ll keep checking hourly!"
        subject = f"No Jobs Found - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    send_email(subject, content)

if __name__ == "__main__":
    main()
