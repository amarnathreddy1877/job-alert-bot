# job_alerts.py

import requests
import json
import re
import os
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -----------------------------
# SMART JOB MATCHING KEYWORDS
# -----------------------------
ROLE_KEYWORDS = [
    "data analyst", "business analyst", "analytics", "bi analyst", "product analyst",
    "reporting analyst", "research analyst", "marketing analyst"
]

SKILL_KEYWORDS = [
    "sql", "python", "r", "excel", "tableau", "power bi", "looker", "dashboard",
    "data visualization", "etl", "bigquery", "snowflake", "data wrangling",
    "pandas", "numpy", "statistics", "predictive modeling", "regression", "ab testing"
]

# Load companies list

def load_companies():
    with open("companies.json", "r") as fh:
        return json.load(fh)

# Job matching logic

def is_data_analyst_job(title, description):
    title = title.lower()
    description = description.lower()
    
    # Match role keyword in title
    if not any(role in title for role in ROLE_KEYWORDS):
        return False

    # Match at least 2 skill keywords in description
    skill_matches = sum(1 for skill in SKILL_KEYWORDS if skill in description)
    return skill_matches >= 1

# Scrape jobs from each company

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

                # Fetch the job page to get description
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

# Send email using SendGrid

def send_email(jobs):
    if not jobs:
        print("No matched jobs to email.")
        return

    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
    SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")

    content = "".join(
        f"- {job['company']}: [{job['title']}]({job['url']}) - {job['location']}\n"
        for job in jobs
    )

    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=RECIPIENT_EMAIL,
        subject="🧠 Data Analyst Job Alerts",
        plain_text_content=content,
        html_content=content.replace("\n", "<br>")
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        print("SendGrid status:", response.status_code)
    except Exception as e:
        print("Send error:", e)

# Main

def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)
    send_email(jobs)

if __name__ == "__main__":
    main()
