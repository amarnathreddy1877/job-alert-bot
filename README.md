# Job-Alert Bot  (H-1B-friendly Data-Analyst roles)

Hourly (8 AM – 5 PM ET) cron job that:
1. Scrapes H-1B-sponsoring career pages (`companies.json`)
2. Filters entry-/mid-level data-analytics roles in the USA
3. Emails grouped results via Mailjet

## Environment vars (set on Render)
MJ_API_KEY       – 462fa7428ea8f812466f212364a91267  
MJ_API_SECRET    – dced41735c848299e049ab4f26f7518e  
SENDER_EMAIL     – verified sender (e.g. alerts@yourdomain.com)  
RECIPIENT_EMAIL  – amarnathreddymalkireddy@gmail.com  

Edit `companies.json` any time; next run picks it up automatically.
