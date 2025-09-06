import os
import re
import time
import datetime as dt
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# ---------------------------------------------------------------------------
# Load company list (job boards only)
# ---------------------------------------------------------------------------
import json

with open("companies.json") as f:
    companies = json.load(f)

# ---------------------------------------------------------------------------
# Keyword Matching
# ---------------------------------------------------------------------------
def is_data_analyst_job(title: str, desc: str) -> bool:
    title = re.sub(r"\s+", " ", title).strip().lower()
    return "analyst" in title

# ---------------------------------------------------------------------------
# Scraper Functions for Each Job Board
# ---------------------------------------------------------------------------
def scrape_remoteok(url: str) -> List[Dict]:
    jobs = []
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    rows = soup.find_all("tr", class_="job")
    for row in rows:
        title_el = row.find("h2")
        link_el = row.find("a", class_="preventLink")
        if title_el and link_el:
            title = title_el.get_text(strip=True)
            link = "https://remoteok.com" + link_el.get("href")
            if is_data_analyst_job(title, title):
                jobs.append({"title": title, "link": link})
    return jobs

def scrape_wellfound(url: str) -> List[Dict]:
    jobs = []
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    job_cards = soup.find_all("div", class_="styles_component__mcpF0")
    for card in job_cards:
        a_tag = card.find("a")
        if a_tag:
            title = a_tag.get_text(strip=True)
            link = "https://wellfound.com" + a_tag.get("href")
            if is_data_analyst_job(title, title):
                jobs.append({"title": title, "link": link})
    return jobs

def scrape_ycombinator(url: str) -> List[Dict]:
    jobs = []
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    cards = soup.find_all("a", class_="job-title")
    for a in cards:
        title = a.get_text(strip=True)
        link = "https://www.ycombinator.com" + a.get("href")
        if is_data_analyst_job(title, title):
            jobs.append({"title": title, "link": link})
    return jobs

def scrape_levels(url: str) -> List[Dict]:
    jobs = []
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    cards = soup.find_all("a", class_="job-title")
    for a in cards:
        title = a.get_text(strip=True)
        link = a.get("href")
        if not link.startswith("http"):
            link = "https://www.levels.fyi" + link
        if is_data_analyst_job(title, title):
            jobs.append({"title": title, "link": link})
    return jobs

# ---------------------------------------------------------------------------
# Master Scraper Dispatcher
# ---------------------------------------------------------------------------
def scrape_jobs(companies: List[Dict]) -> Dict[str, List[Dict]]:
    all_jobs = {}
    for company in companies:
        name = company["name"]
        url = company["url"]
        print(f"\U0001F50D Scraping jobs for: {name} — {url}")
        try:
            if "remoteok" in url:
                jobs = scrape_remoteok(url)
            elif "wellfound" in url:
                jobs = scrape_wellfound(url)
            elif "ycombinator" in url:
                jobs = scrape_ycombinator(url)
            elif "levels" in url:
                jobs = scrape_levels(url)
            else:
                jobs = []
            print(f"[{name}] Found {len(jobs)} matched jobs")
            all_jobs[name] = jobs
        except Exception as e:
            print(f"[{name}] Error: {e}")
            all_jobs[name] = []
    return all_jobs

# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------
SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]

def build_email(jobs_by_company: Dict[str, List[Dict]]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs (Recruiter Ready)"
    if not any(jobs_by_company.values()):
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    sections: List[str] = []
    for comp, jobs in sorted(jobs_by_company.items()):
        if not jobs:
            continue
        sections.append(f"<h3>{comp}</h3><ul>")
        for j in jobs:
            sections.append(f"<li><a href='{j['link']}'>{j['title']}</a></li>")
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
        raise RuntimeError(f"SendGrid error {response.status_code}: {response.body}")

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    jobs_by_company = scrape_jobs(companies)
    print(f"\n\U0001F4DD Summary: {sum(len(v) for v in jobs_by_company.values())} jobs matched across {len(jobs_by_company)} companies.")
    email = build_email(jobs_by_company)
    send_email(email)
