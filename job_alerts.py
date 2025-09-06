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
# Load Companies
# ---------------------------------------------------------------------------

def load_companies(path: str = "companies.json") -> List[Dict]:
    with open(path, "r") as f:
        return json.load(f)

# ---------------------------------------------------------------------------
# Scrape Jobs Function
# ---------------------------------------------------------------------------

def scrape_jobs(companies: List[Dict]) -> List[Dict]:
    jobs = []
    for company in companies:
        name = company["name"]
        url = company["url"]
        print(f"🔍 Scraping jobs for: {name} — {url}")
        try:
            res = requests.get(url, timeout=15)
            soup = BeautifulSoup(res.text, "html.parser")

            anchors = soup.find_all("a", href=True)
            total_links = len(anchors)
            print(f"[{name}] Found {total_links} total links")

            matched = 0
            for a in anchors:
                title = a.get_text(strip=True)
                if not title or len(title) > 120 or len(title.split()) < 2:
                    continue

                title_clean = re.sub(r"\s+", " ", title).strip().lower()
                href = a["href"]
                job_url = href if href.startswith("http") else url.rstrip("/") + "/" + href.lstrip("/")

                # Match any job titles containing "analyst"
                if "analyst" in title_clean:
                    jobs.append({
                        "title": title.strip(),
                        "url": job_url,
                        "company": name
                    })
                    matched += 1

            print(f"[{name}] Found {matched} matched jobs")
        except Exception as e:
            print(f"[{name}] Error scraping: {e}")
            continue
    return jobs

# ---------------------------------------------------------------------------
# Email builder & sender (SendGrid)
# ---------------------------------------------------------------------------

SENDGRID_API_KEY = os.environ["SENDGRID_API_KEY"]


def build_email(jobs: List[Dict]) -> Dict[str, str]:
    now = dt.datetime.now(dt.timezone.utc).astimezone()
    subject = f"[{now:%-I %p}] New Data Analyst Jobs"

    if not jobs:
        html = "<p>No new jobs found this hour.</p>"
        return {"subject": subject, "html": html}

    grouped: Dict[str, List[Dict]] = {}
    for job in jobs:
        grouped.setdefault(job["company"], []).append(job)

    sections: List[str] = []
    for comp, comp_jobs in sorted(grouped.items()):
        sections.append(f"<h3>{comp}</h3><ul>")
        for j in comp_jobs:
            sections.append(f"<li><a href='{j['url']}'>{j['title']}</a></li>")
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
# Main Logic
# ---------------------------------------------------------------------------

def main():
    companies = load_companies()
    jobs = scrape_jobs(companies)
    email = build_email(jobs)
    send_email(email)


if __name__ == "__main__":
    main()
