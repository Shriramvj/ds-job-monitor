#!/usr/bin/env python3
"""DS Job Monitor - Shriram Vijaykumar
Scans Greenhouse, Lever, Ashby for new DS/Analytics/BI/Insights roles.
"""
import json, os, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

CACHE_FILE  = "job_cache.json"
OUTPUT_FILE = "new_jobs.md"
GITHUB_ENV  = os.environ.get("GITHUB_ENV", "")
TIMEOUT, MAX_WORKERS = 10, 25

MATCH = ["data scientist","data science","data analyst","analytics analyst",
    "business intelligence","bi analyst","bi engineer","insights analyst",
    "insights engineer","consumer insights","marketing analyst","market analyst",
    "market intelligence","analytics engineer","analytics manager",
    "strategy analyst","decision science","quantitative analyst","quant analyst",
    "product analyst","growth analyst","revenue analyst","commercial analyst",
    "reporting analyst","performance analyst","people analytics","hr analytics"]
EXCLUDE = ["staff ","principal ","director","vp ","vice president","head of",
    "chief","manager of","senior manager","software engineer","frontend",
    "backend","devops","security","legal","recruiter","designer","ux ","account executive"]

GREENHOUSE = ["airbnb","stripe","databricks","figma","robinhood","brex","plaid",
    "coinbase","reddit","instacart","doordash","lyft","pinterest","dropbox","twilio",
    "zendesk","hubspot","cloudflare","datadog","elastic","confluent","amplitude",
    "mixpanel","dbtlabs","fivetran","hightouch","mode","sigma","hex","modernhealth",
    "tempus","color","headspace","commure","komodohealth","chime","mercury","ramp",
    "marqeta","blend","wayfair","poshmark","vroom","hims","duolingo","coursera",
    "attentive","klaviyo","iterable","braze","benchling","lattice","rippling","deel",
    "remote","drata","vanta","scaleai","labelbox","snorkelai","patientpoint","veeva"]
LEVER = ["netflix","github","notion","airtable","webflow","canva","miro","loom",
    "zapier","intercom","contentful","heap","fullstory","pendo","productboard",
    "hotjar","surveymonkey","qualtrics","wealthsimple","nerdwallet","avant","arcadia",
    "metabase","preset","lightdash","airwallex","typeform","lattice","squarespace",
    "shutterstock","invision","lucid","whimsical","coda"]
ASHBY = ["dbt-labs","airbyte","elementary-data","metaplane","datafold","turntable",
    "steep","chalk","cohere","together-ai","anyscale","weights-biases","gretel",
    "scale-ai","linear","retool","vercel","posthog","mercury","ramp","brex","puzzle",
    "middesk","check","gusto","remote","deel","rippling","headway","alma","brightline",
    "nourish","lyra-health","spring-health","cerebral","beehiiv","read-ai","reforge","luma"]

def is_match(t,d=""):
    s=(t+" "+d).lower()
    return any(k in s for k in MATCH) and not any(k in s for k in EXCLUDE)

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f: return set(json.load(f).get("seen_ids",[]))
    return set()

def save_cache(seen):
    with open(CACHE_FILE,"w") as f:
        json.dump({"seen_ids":list(seen),"last_run":datetime.utcnow().isoformat()},f,indent=2)

def fetch_gh(co):
    try:
        r=requests.get(f"https://boards-api.greenhouse.io/v1/boards/{co}/jobs",timeout=TIMEOUT)
        if r.status_code!=200: return []
        out=[]
        for j in r.json().get("jobs",[]):
            t=j.get("title",""); d=(j.get("departments") or [{}])[0].get("name","")
            if is_match(t,d): out.append({"id":f"gh_{j['id']}","title":t,
                "company":co.replace("-"," ").title(),"location":j.get("location",{}).get("name",""),
                "url":j.get("absolute_url",""),"posted":(j.get("updated_at") or "")[:10],"source":"Greenhouse"})
        return out
    except: return []

def fetch_lv(co):
    try:
        r=requests.get(f"https://api.lever.co/v0/postings/{co}?mode=json&limit=250",timeout=TIMEOUT)
        if r.status_code!=200: return []
        out=[]
        for j in (r.json() if isinstance(r.json(),list) else []):
            t=j.get("text",""); d=j.get("categories",{}).get("department","")
            ts=j.get("createdAt",0)
            if is_match(t,d): out.append({"id":f"lv_{j['id']}","title":t,
                "company":co.replace("-"," ").title(),"location":j.get("categories",{}).get("location",""),
                "url":j.get("hostedUrl",""),
                "posted":datetime.utcfromtimestamp(ts/1000).strftime("%Y-%m-%d") if ts else "","source":"Lever"})
        return out
    except: return []

def fetch_ab(co):
    try:
        r=requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{co}",timeout=TIMEOUT)
        if r.status_code!=200: return []
        out=[]
        for j in r.json().get("jobPostings",[]):
            t=j.get("title",""); d=j.get("departmentName","")
            if is_match(t,d): out.append({"id":f"ab_{j['id']}","title":t,
                "company":co.replace("-"," ").title(),"location":j.get("locationName",""),
                "url":j.get("jobUrl",""),"posted":(j.get("publishedDate") or "")[:10],"source":"Ashby"})
        return out
    except: return []

def run():
    print(f"DS Job Monitor - {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    seen=load_cache(); new_jobs=[]
    tasks=[("gh",c) for c in GREENHOUSE]+[("lv",c) for c in LEVER]+[("ab",c) for c in ASHBY]
    print(f"Scanning {len(tasks)} boards...")
    fns={"gh":fetch_gh,"lv":fetch_lv,"ab":fetch_ab}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs={ex.submit(fns[s],c):c for s,c in tasks}
        for f in as_completed(futs):
            for job in f.result():
                if job["id"] not in seen:
                    new_jobs.append(job); seen.add(job["id"])
    new_jobs.sort(key=lambda j:(j["source"],j["company"],j["title"]))
    save_cache(seen)
    count=len(new_jobs)
    print(f"{count} new role(s) found.")
    if count:
        lines=[f"# {count} New DS/Analytics Role(s)",
            f"Scanned: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} | Greenhouse + Lever + Ashby","",
            "| # | Title | Company | Location | Posted | Source | Link |",
            "|---|-------|---------|----------|--------|--------|------|"]
        for i,j in enumerate(new_jobs,1):
            lines.append(f"| {i} | **{j['title']}** | {j['company']} | {j['location'] or '-'} | {j['posted'] or '-'} | {j['source']} | [Apply]({j['url']}) |")
        lines+=["","---","*ds-job-monitor*"]
        with open(OUTPUT_FILE,"w") as f: f.write("\n".join(lines))
        for j in new_jobs[:5]: print(f"  [{j['source']}] {j['title']} @ {j['company']}")
    else:
        with open(OUTPUT_FILE,"w") as f: f.write(f"No new roles.\nChecked: {datetime.utcnow().isoformat()}\n")
    if GITHUB_ENV:
        with open(GITHUB_ENV,"a") as f: f.write(f"NEW_JOBS_COUNT={count}\n")

if __name__=="__main__":
    run()
