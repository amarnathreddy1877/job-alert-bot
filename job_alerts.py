import os
import re
import json
import requests
from bs4 import BeautifulSoup

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
FROM_EMAIL = os.getenv("FROM_EMAIL")
TO_EMAIL = os.getenv("TO_EMAIL")

# --- Job Filter Function ---
def is_data_analyst_job(title, description):
    title = title.lower()
    description = description.lower()

    if "analyst" in title:
        return True

    SKILL_KEYWORDS = [
        "sql", "python", "r", "tableau", "power bi", "looker",
        "data visualization", "dashboard", "etl", "data pipeline",
        "statistics", "machine learning", "data modeling", "big data"
    ]

    skills_matched = [kw for kw in SKILL_KEYWORDS if kw in description]
    return len(skills_matched) >= 2

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
def send_email(jobs, no_matches=False):
    content = ""

    if no_matches:
        content = "<b>No data analyst jobs were found in this hour’s scan.</b><br>We’ll keep checking hourly!"
    else:
        for job in jobs:
            content += f"🧠 <b>{job['title']}</b><br>"
            content += f"🏢 {job['company']}<br>"
            content += f"🌍 {job['location']}<br>"
            content += f"🔗 <a href='{job['url']}'>Apply Here</a><br><br>"

    message = {
        "personalizations": [{"to": [{"email": TO_EMAIL}]}],
        "from": {"email": FROM_EMAIL},
        "subject": "📬 Hourly Data Analyst Job Update",
        "content": [{"type": "text/html", "value": content}],
    }

    response = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={
            "Authorization": f"Bearer {SENDGRID_API_KEY}",
            "Content-Type": "application/json",
        },
        json=message,
    )

    print("SendGrid status:", response.status_code)
    if response.status_code != 202:
        print("Response:", response.text)

# --- Load Company List ---
def load_companies():
    with open("companies.json") as fh:
        return json.load(fh)

# --- Main Entry ---
def main():
    companies = load_companies()
    matched_jobs = scrape_jobs(companies)

    if matched_jobs:
        send_email(matched_jobs)
    else:
        print("No matched jobs to email.")
        send_email([], no_matches=True)

if __name__ == "__main__":
    main()
