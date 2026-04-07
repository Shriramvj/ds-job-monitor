#!/usr/bin/env python3
"""DS Job Monitor - Shriram Vijaykumar
Scans Greenhouse, Lever, Ashby for new DS/Analytics/BI/Insights roles.
USA only · <5 years experience (no Director/VP/Principal) · Opens GitHub Issue + sends email.
"""
import json, os, re, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

CACHE_FILE  = "job_cache.json"
OUTPUT_FILE = "new_jobs.md"
GITHUB_ENV  = os.environ.get("GITHUB_ENV", "")
TIMEOUT, MAX_WORKERS = 10, 25

# ── Title match (expanded per Shriram's list) ─────────────────────────────────
MATCH = [
    "business analyst","data scientist","product data scientist",
    "applied data scientist","decision scientist","quantitative analyst",
    "product analyst","analytics engineer","business intelligence analyst",
    "bi analyst","bi engineer","decision science","customer insights analyst",
    "consumer insights","growth analyst","revenue analytics","revenue analyst",
    "experimentation scientist","a/b testing","causal inference",
    "measurement scientist","applied statistician","marketing data scientist",
    "marketing scientist","media mix model","mmm scientist",
    "attribution scientist","crm analytics","personalization scientist",
    "real world evidence","rwe scientist","patient journey analyst",
    "healthcare data scientist","pharma analytics","heor analyst",
    "health economics","outcomes research","insights analyst",
    "insights engineer","market analyst","market intelligence",
    "reporting analyst","performance analyst","people analytics","hr analytics",
]

# ── Exclude — too senior / irrelevant / >5 yrs ───────────────────────────────
EXCLUDE = [
    "director","vp ","vice president","head of","chief","principal ",
    "staff ","senior manager","manager of","associate director",
    "software engineer","frontend","backend","devops","security",
    "legal","recruiter","designer","ux ","account executive",
]

# ── USA location check ────────────────────────────────────────────────────────
US_STATES = {
    "alabama","alaska","arizona","arkansas","california","colorado",
    "connecticut","delaware","florida","georgia","hawaii","idaho",
    "illinois","indiana","iowa","kansas","kentucky","louisiana","maine",
    "maryland","massachusetts","michigan","minnesota","mississippi",
    "missouri","montana","nebraska","nevada","new hampshire","new jersey",
    "new mexico","new york","north carolina","north dakota","ohio",
    "oklahoma","oregon","pennsylvania","rhode island","south carolina",
    "south dakota","tennessee","texas","utah","vermont","virginia",
    "washington","west virginia","wisconsin","wyoming","district of columbia",
    "al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in",
    "ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv",
    "nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn",
    "tx","ut","vt","va","wa","wv","wi","wy","dc",
    "san francisco","new york","los angeles","chicago","seattle","boston",
    "austin","denver","atlanta","miami","dallas","houston","portland",
    "san diego","san jose","brooklyn","manhattan","remote",
}
NON_US = [
    "canada","uk","united kingdom","london","toronto","vancouver",
    "india","bangalore","hyderabad","germany","berlin","france","paris",
    "australia","sydney","singapore","brazil","mexico","ireland","dublin",
    "netherlands","amsterdam","spain","poland","israel","tel aviv",
]

def is_usa(location: str) -> bool:
    """True if location is US-based, US-remote, or blank (assume US)."""
    if not location or location.strip() == "":
        return True
    loc = location.lower()
    for skip in NON_US:
        if skip in loc:
            return False
    if any(kw in loc for kw in ("united states","usa","u.s.","u.s.a")):
        return True
    tokens = set(re.split(r"[,\s/|()]+", loc))
    return bool(tokens & US_STATES)

def is_match(title: str, location: str) -> bool:
    t = title.lower()
    if not any(k in t for k in MATCH):
        return False
    if any(k in t for k in EXCLUDE):
        return False
    if not is_usa(location):
        return False
    return True

# ── Cache ─────────────────────────────────────────────────────────────────────
def load_cache():
    try:
        return set(json.load(open(CACHE_FILE)))
    except Exception:
        return set()

def save_cache(seen):
    with open(CACHE_FILE, "w") as f:
        json.dump(list(seen), f)

# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_gh(company):
    try:
        url  = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
        data = requests.get(url, timeout=TIMEOUT).json()
        jobs = []
        for j in data.get("jobs", []):
            title = j.get("title", "")
            loc   = j.get("location", {}).get("name", "")
            if is_match(title, loc):
                jobs.append({
                    "id": f"gh-{j['id']}", "title": title,
                    "company": company,    "location": loc,
                    "url": j.get("absolute_url", ""), "source": "Greenhouse",
                })
        return jobs
    except Exception:
        return []

def fetch_lv(company):
    try:
        url  = f"https://api.lever.co/v0/postings/{company}?mode=json&limit=250"
        data = requests.get(url, timeout=TIMEOUT).json()
        jobs = []
        for j in data:
            title = j.get("text", "")
            loc   = j.get("categories", {}).get("location", "")
            if is_match(title, loc):
                jobs.append({
                    "id": f"lv-{j['id']}", "title": title,
                    "company": company,    "location": loc,
                    "url": j.get("hostedUrl", ""), "source": "Lever",
                })
        return jobs
    except Exception:
        return []

def fetch_ab(company):
    try:
        url  = f"https://api.ashbyhq.com/posting-api/job-board/{company}"
        data = requests.get(url, timeout=TIMEOUT).json()
        jobs = []
        for j in data.get("jobPostings", []):
            title = j.get("title", "")
            loc   = j.get("location", "") or j.get("locationName", "")
            if is_match(title, loc):
                jobs.append({
                    "id": f"ab-{j['id']}", "title": title,
                    "company": company,    "location": loc,
                    "url": j.get("jobUrl", ""), "source": "Ashby",
                })
        return jobs
    except Exception:
        return []

# ── Company lists ─────────────────────────────────────────────────────────────
GREENHOUSE = [
    "airbnb","stripe","databricks","figma","robinhood","brex","plaid",
    "coinbase","reddit","instacart","doordash","lyft","pinterest","dropbox","twilio",
    "zendesk","hubspot","cloudflare","datadog","elastic","confluent","amplitude",
    "mixpanel","dbtlabs","fivetran","hightouch","mode","sigma","hex","modernhealth",
    "tempus","color","headspace","commure","komodohealth","chime","mercury","ramp",
    "marqeta","blend","wayfair","poshmark","vroom","hims","duolingo","coursera",
    "attentive","klaviyo","iterable","braze","benchling","lattice","rippling","deel",
    "remote","drata","vanta","scaleai","labelbox","snorkelai","patientpoint","veeva",
]
LEVER = [
    "netflix","github","notion","airtable","webflow","canva","miro","loom",
    "zapier","intercom","contentful","heap","fullstory","pendo","productboard",
    "hotjar","surveymonkey","qualtrics","wealthsimple","nerdwallet","avant","arcadia",
    "metabase","preset","lightdash","airwallex","typeform","squarespace",
    "shutterstock","invision","lucid","whimsical","coda",
]
ASHBY = [
    "dbt-labs","airbyte","elementary-data","metaplane","datafold","turntable",
    "steep","chalk","cohere","together-ai","anyscale","weights-biases","gretel",
    "scale-ai","linear","retool","vercel","posthog","mercury","ramp","brex","puzzle",
    "middesk","check","gusto","remote","deel","rippling","headway","alma","brightline",
    "nourish","lyra-health","spring-health","cerebral","beehiiv","read-ai","reforge","luma",
]

# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    seen  = load_cache()
    tasks = (
        [(fetch_gh, c) for c in GREENHOUSE] +
        [(fetch_lv, c) for c in LEVER] +
        [(fetch_ab, c) for c in ASHBY]
    )
    all_jobs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fn, c): c for fn, c in tasks}
        for f in as_completed(futs):
            all_jobs.extend(f.result())

    # Filter to truly new jobs and dedup
    seen_ids, new_jobs = set(), []
    for j in all_jobs:
        if j["id"] not in seen and j["id"] not in seen_ids:
            seen_ids.add(j["id"])
            new_jobs.append(j)

    print(f"Scanned: {len(all_jobs)}  |  New USA matches: {len(new_jobs)}")

    if new_jobs:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        lines = [
            f"# {len(new_jobs)} New DS/Analytics Jobs — USA Only — {ts}",
            "",
            "| # | Title | Company | Location | Source | Apply |",
            "|---|-------|---------|----------|--------|-------|",
        ]
        for i, j in enumerate(new_jobs, 1):
            lines.append(
                f"| {i} | {j['title']} | {j['company']} | {j['location']} "
                f"| {j['source']} | [Apply]({j['url']}) |"
            )
        with open(OUTPUT_FILE, "w") as f:
            f.write("\n".join(lines))
    else:
        with open(OUTPUT_FILE, "w") as f:
            f.write("No new matching US jobs this run.")

    # Update cache
    for j in all_jobs:
        seen.add(j["id"])
    save_cache(seen)

    count = len(new_jobs)
    if GITHUB_ENV:
        with open(GITHUB_ENV, "a") as f:
            f.write(f"NEW_JOBS_COUNT={count}\n")
    print(f"Done. {count} new jobs written to {OUTPUT_FILE}.")

if __name__ == "__main__":
    run()
