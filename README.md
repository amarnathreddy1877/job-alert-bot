# Job Alerts — Entry/Mid Data Analyst (US)

Hourly email digest of new **data analyst** roles (US-based, entry–mid level).  
Sources: public ATS APIs (**Greenhouse, Lever, SmartRecruiters**).  
Delivery: **SendGrid** (HTML email).

---

## How it works
- Pulls listings from companies in `companies.json`
- Filters for analyst keywords and excludes senior/manager/director
- Filters to US or **Remote (US)** roles
- De-duplicates using a cache (`.cache/seen_ids.json`)
- Emails a grouped HTML digest via SendGrid
- Runs hourly via GitHub Actions

---

## Setup (GitHub-only)
1. **Fork or use this repo.**
2. **Create SendGrid Free account**
   - Verify a **single sender** (the Gmail you will send from).
   - Create an **API Key** with “Full Access” to Mail Send.
3. **Add Action Secrets (Repo → Settings → Secrets & variables → Actions)**
   - `SENDGRID_API_KEY` → your key
   - `SENDER_EMAIL` → the verified sender email (must match SendGrid Sender)
   - `RECIPIENT_EMAIL` → where you want alerts
4. Optional: Edit `companies.json` to add/remove boards.
5. Trigger manually once: **Actions → Job Alerts (Hourly) → Run workflow**.

> **Tip:** If you get **401 Unauthorized** from SendGrid:
> - Ensure the API key is correct and not expired.
> - Ensure **SENDER_EMAIL** is a verified sender in SendGrid.
> - Try creating a new API key and updating the secret.

---

## Customize filters
- Positive/negative keyword logic lives in `job_alerts.py`:
  - `KEYWORDS_POSITIVE` — add tools (“dbt”, “Power BI”, “Tableau”, “SQL”, etc.)
  - `KEYWORDS_NEGATIVE` — keep out “Senior/Lead/Director/Manager”
- US filter uses a state/country regex and accepts “Remote - US”.

---

## Add more companies

### Greenhouse
```json
{ "name": "YourCo", "type": "greenhouse", "board": "yourco" }
