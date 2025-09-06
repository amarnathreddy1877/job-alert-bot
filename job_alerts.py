import os
import re
import datetime as dt
import requests
from bs4 import BeautifulSoup
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import json
from typing import List, Dict

# ---------------------------------------------------------------------------
# Keywords & Utilities
# ---------------------------------------------------------------------------

def is_data_analyst_job(title: str, desc: str) -> bool:
    title = re.sub("\s+", " ", title).strip().lower()
    desc = desc.lower()
    return (
        "analyst" in title
        and not any(x in title for x in ["senior", "principal", "lead", "manager", "director"])
    )

# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------

def scrape_monster_jobs(company: Dict) -> List[Dict]:
    print(f"🔍 Scraping jobs for: {company['name']} — {company['url']}")
    jobs = []
    try:
        res = requests.get(company["url"], timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        cards = soup.select("section.card-content")
        print(f"[{company['name']}] Found {len(cards)} total jobs")
        for card in cards:
            title_tag = card.find("h2")
            link_tag = card.find("a", href=True)
            if title_tag and link_tag:
                title = title_tag.get_text(strip=True)
                link = link_tag["href"]
                job_desc = title  # Monster doesn't give detail in card, use title only
                if is_data_analyst_job(title, job_desc):
                    jobs.append({"title": title, "link": link})
        print(f"[{company['name']}] Found {len(jobs)} matched jobs")
    except Exception as e:
        print(f"[{company['name']}] Error scraping: {e}")
    return jobs

def scrape_dice_jobs(company: Dict) -> List[Dict]:
    print(f"🔍 Scraping jobs for: {company['name']} — {company['url']}")
    jobs = []
    try:
        res = requests.get(company["url"], timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(res.text, "html.parser")
        cards = soup.select(".card-title-link")
        print(f"[{company['name']}] Found {len(cards)} total jobs")
        for a in cards:
            title = a.get_text(strip=True)
            link = a["href"]
            job_desc = title  # Title only
            if is_data_analyst_job(title, job_desc):
                jobs.append({"title": title, "link": link})
        print(f"[{company['name']}] Found {len(jobs)} matched jobs")
    except Exception as e:
        print(f"[{company['name']}] Error scraping: {e}")
    return jobs

# Add more scrapers below if needed...

# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------
SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL")

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
            sections.append(f"<li><a href='{j['link']}'>{j['title']}</a></li>")
        sections.append("</ul>")
    html = "\n".join(sections)
    return {"subject": subject, "html": html}

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
# Main Execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    with open("companies.json") as f:
        companies = json.load(f)

    all_jobs: Dict[str, List[Dict]] = {}

    for company in companies:
        name = company["name"].lower()
        if "monster" in name:
            jobs = scrape_monster_jobs(company)
        elif "dice" in name:
            jobs = scrape_dice_jobs(company)
        else:
            print(f"Skipping {company['name']}: no scraper implemented.")
            jobs = []
        all_jobs[company["name"]] = jobs

    summary = sum(len(j) for j in all_jobs.values())
    print(f"📝 Summary: {summary} jobs matched across {len(all_jobs)} companies.")

    email = build_email(all_jobs)
    send_email(email)
