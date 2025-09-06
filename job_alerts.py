import os
import re
import json
import requests
import datetime as dt
from typing import List, Dict
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# -----------------------------------------------------------------------------
# Job matching function (simple keyword in title)
# -----------------------------------------------------------------------------

def is_data_analyst_job(title: str, description: str) -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    return "analyst" in title

# -----------------------------------------------------------------------------
# Scraper for Dice
# -----------------------------------------------------------------------------

def scrape_dice_jobs(company: Dict[str, str]) -> List[Dict[str, str]]:
    jobs = []
    url = company["url"]
    print(f"🔍 Scraping jobs for: {company['name']} — {url}")

    try:
        res = requests.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        job_cards = soup.find_all("a", class_="card-title-link")
        print(f"[{company['name']}] Found {len(job_cards)} total links")

        count = 0
        for job in job_cards:
            title = job.get_text(strip=True)
            href = job["href"]
            job_url = "https://www.dice.com" + href if href.startswith("/") else href
            description = title  # no separate desc, use title

            if is_data_analyst_job(title, description):
                jobs.append({
                    "title": title,
                    "url": job_url,
                    "company": company["name"]
                })
                count += 1
        print(f"[{company['name']}] Found {count} matched jobs")

    except Exception as e:
        print(f"[{company['name']}] Failed: {e}")

    return jobs

# -----------------------------------------------------------------------------
# Generic scraper (fallback, not ideal for Amazon/Google)
# -----------------------------------------------------------------------------

def scrape_generic_jobs(company: Dict[str, str]) -> List[Dict[str, str]]:
    jobs = []
    url = company["url"]
    print(f"🔍 Scraping jobs for: {company['name']} — {url}")

    try:
        res = requests.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")

        links = soup.find_all("a", href=True)
        print(f"[{company['name']}] Found {len(links)} total links")

        count = 0
        for a in links:
            title = a.get_text(strip=True)
            href = a["href"]
            job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href
            description = title

            if is_data_analyst_job(title, description):
                jobs.append({
                    "title": title,
                    "url": job_url,
                    "company": company["name"]
                })
                count += 1

        print(f"[{company['name']}] Found {count} matched jobs")

    except Exception as e:
        print(f"[{company['name']}] Failed: {e}")

    return jobs

# -----------------------------------------------------------------------------
# Master scrape function
# -----------------------------------------------------------------------------

def scrape_all_jobs(companies: List[Dict[str, str]]) -> Dict[str, List[Dict]]:
    all_jobs = {}

    for company in companies:
        name = company["name"]
        if "dice.com" in company["url"]:
            jobs = scrape_dice_jobs(company)
        else:
            jobs = scrape_generic_jobs(company)
        all_jobs[name] = jobs

    return all_jobs

# -----------------------------------------------------------------------------
# Email builder & sender
# -----------------------------------------------------------------------------

SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
RECIPIENT_EMAIL = os.environ["RECIPIENT_EMAIL"]

def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] Data Analyst Job Alerts"

    if not any(jobs_by_company.values()):
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    sections: List[str] = []
    for company, jobs in sorted(jobs_by_company.items()):
        if not jobs:
            continue
        sections.append(f"<h3>{company}</h3><ul>")
        for job in jobs:
            sections.append(f"<li><a href='{job['url']}'>{job['title']}</a></li>")
        sections.append("</ul>")
    html = "\n".join(sections)
    return {"subject": subject, "html": html}

def send_email(email: Dict[str, str]) -> None:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    message = Mail(
        from_email=SENDER_EMAIL,
        to_emails=RECIPIENT_EMAIL,
        subject=email["subject"],
        html_content=email["html"]
    )
    response = sg.send(message)
    print("SendGrid status:", response.status_code)
    if response.status_code not in (200, 202):
        raise RuntimeError(f"SendGrid error {response.status_code}: {response.body}")

# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

def main():
    with open("companies.json") as f:
        companies = json.load(f)

    jobs_by_company = scrape_all_jobs(companies)
    summary = sum(len(jobs) for jobs in jobs_by_company.values())
    print(f"📝 Summary: {summary} jobs matched across {len(jobs_by_company)} companies.")

    email = build_email(jobs_by_company)
    send_email(email)

if __name__ == "__main__":
    main()
