import os
import re
import datetime as dt
import requests
import json
from typing import List, Dict
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------

def is_analyst_job(title: str) -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    return "analyst" in title


# ------------------------------------------------------------------------------
# Scrapers
# ------------------------------------------------------------------------------

def scrape_amazon_jobs(url: str, name: str) -> List[Dict]:
    jobs = []
    try:
        res = requests.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        for tile in soup.select(".job-tile"):
            title_tag = tile.select_one(".job-title")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)
            link = tile.find("a", href=True)
            href = link["href"] if link else ""
            job_url = "https://www.amazon.jobs" + href if href.startswith("/") else href
            if is_analyst_job(title):
                jobs.append({
                    "title": title,
                    "url": job_url,
                    "company": name,
                    "location": "USA"
                })
    except Exception as e:
        print(f"[Amazon Error] {e}")
    return jobs


def scrape_google_jobs(url: str, name: str) -> List[Dict]:
    jobs = []
    try:
        res = requests.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            href = a["href"]
            if "jobs/results" not in href:
                continue
            job_url = "https://careers.google.com" + href if href.startswith("/") else href
            if is_analyst_job(title):
                jobs.append({
                    "title": title,
                    "url": job_url,
                    "company": name,
                    "location": "USA"
                })
    except Exception as e:
        print(f"[Google Error] {e}")
    return jobs


def scrape_generic_jobs(url: str, name: str) -> List[Dict]:
    jobs = []
    try:
        res = requests.get(url, timeout=15)
        soup = BeautifulSoup(res.text, "html.parser")
        for a in soup.find_all("a", href=True):
            title = a.get_text(strip=True)
            href = a["href"]
            job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href
            if is_analyst_job(title):
                jobs.append({
                    "title": title,
                    "url": job_url,
                    "company": name,
                    "location": "USA"
                })
    except Exception as e:
        print(f"[Generic Error] {e}")
    return jobs


def scrape_jobs(companies: List[Dict[str, str]]) -> List[Dict[str, str]]:
    jobs = []
    for company in companies:
        name = company["name"]
        url = company["url"]
        ctype = company.get("type", "generic")

        print(f"🔍 Scraping jobs for: {name} — {url}")
        if ctype == "amazon":
            company_jobs = scrape_amazon_jobs(url, name)
        elif ctype == "google":
            company_jobs = scrape_google_jobs(url, name)
        else:
            company_jobs = scrape_generic_jobs(url, name)

        print(f"[{name}] Found {len(company_jobs)} matched jobs")
        jobs.extend(company_jobs)

    return jobs


# ------------------------------------------------------------------------------
# Email sender
# ------------------------------------------------------------------------------

def build_email(jobs: List[Dict[str, str]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] Data Analyst Job Alerts (Hourly)"

    if not jobs:
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    grouped = {}
    for job in jobs:
        grouped.setdefault(job["company"], []).append(job)

    sections = []
    for comp, jobs in sorted(grouped.items()):
        sections.append(f"<h3>{comp}</h3><ul>")
        for j in jobs:
            loc = f" – {j['location']}" if j['location'] else ""
            sections.append(f"<li><a href='{j['url']}'>{j['title']}</a>{loc}</li>")
        sections.append("</ul>")

    html = "\n".join(sections)
    return {"subject": subject, "html": html}


def send_email(email: Dict[str, str]) -> None:
    SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
    SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
    RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")

    if not (SENDGRID_API_KEY and SENDER_EMAIL and RECIPIENT_EMAIL):
        raise ValueError("Missing one of SENDGRID_API_KEY, SENDER_EMAIL, or RECIPIENT_EMAIL")

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
        print("Response:", response.body)
        raise RuntimeError(f"SendGrid error {response.status_code}")


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def load_companies() -> List[Dict[str, str]]:
    with open("companies.json", "r") as fh:
        return json.load(fh)


def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)
    email = build_email(jobs)
    send_email(email)


if __name__ == "__main__":
    main()
