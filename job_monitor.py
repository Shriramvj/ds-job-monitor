#!/usr/bin/env python3
"""DS Job Monitor - Shriram Vijaykumar
Scans Greenhouse, Lever, Ashby, Workday, Breezy HR, Workable, iCIMS, Paylocity, ADP
for new DS/Analytics/BI/Insights roles in the USA.
"""
import json, os, re, requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

CACHE_FILE  = "job_cache.json"
OUTPUT_FILE = "new_jobs.md"
GITHUB_ENV  = os.environ.get("GITHUB_ENV", "")
TIMEOUT, MAX_WORKERS = 15, 40

# ── Title match — broad net ────────────────────────────────────────────────────
MATCH = [
    # Core data science / analyst
    "data scientist","data science","applied scientist","research scientist",
    "decision scientist","quantitative analyst","quantitative researcher",
    "business analyst","business intelligence","bi analyst","bi engineer",
    "bi developer","data analyst","analytics analyst","staff analyst",
    # Machine learning / AI
    "machine learning","ml engineer","ml scientist","ml ops","mlops",
    "ml platform","applied ml","ml infrastructure","ml research",
    "ai engineer","ai scientist","ai/ml","artificial intelligence",
    "deep learning","nlp engineer","nlp scientist","natural language processing",
    "computer vision","cv engineer","generative ai","llm engineer",
    "recommendation systems","recommender","ranking engineer","search scientist",
    # Product / growth
    "product analyst","product data scientist","growth analyst",
    "revenue analyst","revenue analytics","customer insights",
    "consumer insights","market analyst","market intelligence",
    "pricing analyst","pricing scientist","demand forecasting",
    # Engineering / platform
    "analytics engineer","data engineer","analytics platform",
    "reporting analyst","reporting engineer","insights analyst",
    "insights engineer","performance analyst","data platform",
    "data infrastructure","data warehouse","data modeling",
    "dbt engineer","spark engineer","data reliability",
    # Visualization / BI tools
    "tableau developer","power bi developer","looker developer",
    "data visualization","dashboard engineer","reporting developer",
    # Experimentation / measurement
    "experimentation scientist","measurement scientist","applied statistician",
    "a/b testing","causal inference","statistician",
    # Operations / supply chain
    "operations analyst","operations research","supply chain analyst",
    "logistics analyst","inventory analyst","demand analyst",
    # Risk / fraud / finance
    "risk analyst","fraud analyst","credit analyst","financial analyst",
    "actuarial analyst","portfolio analyst","quantitative risk",
    # Marketing / CRM
    "marketing analyst","marketing data scientist","marketing scientist",
    "media mix model","mmm scientist","attribution scientist",
    "crm analytics","personalization scientist",
    # Healthcare / pharma
    "real world evidence","rwe scientist","patient journey analyst",
    "healthcare data scientist","pharma analytics","heor analyst",
    "health economics","outcomes research","clinical data analyst",
    "epidemiologist","biostatistician","health informatics",
    # People / HR
    "people analytics","hr analytics","workforce analytics",
]

# ── Exclude — leadership / unrelated ──────────────────────────────────────────
EXCLUDE = [
    "director","vp ","vice president","head of","chief",
    "senior manager","manager of","associate director",
    "software engineer","frontend","backend","devops","security engineer",
    "legal","recruiter","sourcer","designer","ux researcher",
    "account executive","account manager","sales engineer",
    "solutions engineer","solutions architect",
]
# ── USA location check ────────────────────────────────────────────────────────────────
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
    "san diego","san jose","brooklyn","manhattan","remote","united states","usa",
}
NON_US = [
    "canada","uk","united kingdom","london","toronto","vancouver",
    "india","bangalore","hyderabad","pune","chennai","mumbai",
    "germany","berlin","frankfurt","france","paris",
    "australia","sydney","melbourne","singapore","brazil","sao paulo",
    "mexico","mexico city","ireland","dublin","netherlands","amsterdam",
    "spain","madrid","poland","warsaw","israel","tel aviv",
    "sweden","stockholm","denmark","copenhagen","switzerland","zurich",
    "japan","tokyo","china","beijing","shanghai","hong kong","new zealand",
]

def is_usa(location: str) -> bool:
    if not location or location.strip() == "":
        return True   # blank = assume USA (most ATS portals are US-focused)
    loc = location.lower()
    for skip in NON_US:
        if skip in loc:
            return False
    if any(kw in loc for kw in ("united states", "usa", "u.s.", "u.s.a")):
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
# ── Cache ──────────────────────────────────────────────────────────────────────
def load_cache():
    try:
        return set(json.load(open(CACHE_FILE)))
    except Exception:
        return set()

def save_cache(seen):
    with open(CACHE_FILE, "w") as f:
        json.dump(list(seen), f)

# ══════════════════════════════════════════════════════════════════════════════
# FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

# ── GREENHOUSE ────────────────────────────────────────────────────────────────
def fetch_gh(company):
    try:
        data = requests.get(
            f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
            timeout=TIMEOUT
        ).json()
        jobs = []
        for j in data.get("jobs", []):
            title = j.get("title", "")
            loc   = j.get("location", {}).get("name", "")
            if is_match(title, loc):
                jobs.append({"id": f"gh-{j['id']}", "title": title,
                              "company": company, "location": loc,
                              "url": j.get("absolute_url", ""), "source": "Greenhouse"})
        return jobs
    except Exception:
        return []

# ── LEVER ─────────────────────────────────────────────────────────────────────
def fetch_lv(company):
    try:
        data = requests.get(
            f"https://api.lever.co/v0/postings/{company}?mode=json&limit=250",
            timeout=TIMEOUT
        ).json()
        jobs = []
        for j in data:
            title = j.get("text", "")
            loc   = j.get("categories", {}).get("location", "")
            if is_match(title, loc):
                jobs.append({"id": f"lv-{j['id']}", "title": title,
                              "company": company, "location": loc,
                              "url": j.get("hostedUrl", ""), "source": "Lever"})
        return jobs
    except Exception:
        return []

# ── ASHBY ─────────────────────────────────────────────────────────────────────
def fetch_ab(company):
    try:
        data = requests.get(
            f"https://api.ashbyhq.com/posting-api/job-board/{company}",
            timeout=TIMEOUT
        ).json()
        jobs = []
        for j in data.get("jobPostings", []):
            title = j.get("title", "")
            loc   = j.get("location", "") or j.get("locationName", "")
            if is_match(title, loc):
                jobs.append({"id": f"ab-{j['id']}", "title": title,
                              "company": company, "location": loc,
                              "url": j.get("jobUrl", ""), "source": "Ashby"})
        return jobs
    except Exception:
        return []

# ── WORKDAY ───────────────────────────────────────────────────────────────────
def fetch_wd(tenant, job_board="External"):
    """POST to Workday job board API with full pagination (no 20-job cap)."""
    try:
        url    = f"https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{tenant}/{job_board}/jobs"
        limit  = 20   # Workday max per page
        offset = 0
        jobs   = []
        while True:
            resp = requests.post(
                url,
                json={"limit": limit, "offset": offset,
                      "searchText": "", "appliedFacets": {}},
                headers={"Content-Type": "application/json"},
                timeout=TIMEOUT
            )
            if resp.status_code != 200:
                break
            data     = resp.json()
            postings = data.get("jobPostings", [])
            if not postings:
                break
            for j in postings:
                title = j.get("title", "")
                loc   = j.get("locationsText", "")
                path  = j.get("externalPath", "")
                if is_match(title, loc):
                    jobs.append({
                        "id":       f"wd-{tenant}-{path}",
                        "title":    title,
                        "company":  tenant,
                        "location": loc,
                        "url":      f"https://{tenant}.wd1.myworkdayjobs.com{path}",
                        "source":   "Workday",
                    })            # Stop if we got fewer than a full page (last page)
            if len(postings) < limit:
                break
            offset += limit
            # Safety cap: don't scan > 500 jobs per company
            if offset >= 500:
                break
        return jobs
    except Exception:
        return []

# ── BREEZY HR ─────────────────────────────────────────────────────────────────
def fetch_breezy(slug):
    """GET the public JSON feed every Breezy HR company exposes at {slug}.breezy.hr/json"""
    try:
        resp = requests.get(f"https://{slug}.breezy.hr/json", timeout=TIMEOUT)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if not isinstance(data, list):
            return []
        jobs = []
        for j in data:
            if j.get("state", "published") != "published":
                continue
            title   = j.get("name", "")
            loc_obj = j.get("location", {})
            loc     = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
            jid     = j.get("_id", "")
            if is_match(title, loc):
                jobs.append({
                    "id":       f"breezy-{slug}-{jid}",
                    "title":    title,
                    "company":  slug,
                    "location": loc,
                    "url":      f"https://{slug}.breezy.hr/p/{jid}",
                    "source":   "BreezyHR",
                })
        return jobs
    except Exception:
        return []

# ── WORKABLE ──────────────────────────────────────────────────────────────────
def fetch_workable(slug):
    """GET Workable's public widget API for a company slug."""
    try:
        resp = requests.get(
            f"https://apply.workable.com/api/v1/widget/accounts/{slug}/jobs",
            timeout=TIMEOUT
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs = []
        for j in data.get("results", []):
            title     = j.get("title", "")
            loc_obj   = j.get("location", {})
            city      = loc_obj.get("city", "")    if isinstance(loc_obj, dict) else ""
            country   = loc_obj.get("country", "") if isinstance(loc_obj, dict) else ""
            loc       = ", ".join(filter(None, [city, country]))
            shortcode = j.get("shortcode", "")
            if is_match(title, loc):
                jobs.append({
                    "id":       f"wk-{slug}-{shortcode}",
                    "title":    title,
                    "company":  slug,
                    "location": loc,                    "url":      f"https://apply.workable.com/{slug}/j/{shortcode}/",
                    "source":   "Workable",
                })
        return jobs
    except Exception:
        return []

# ── iCIMS ─────────────────────────────────────────────────────────────────────
def fetch_icims(portal_base, company_name):
    """Pull iCIMS RSS job feed (publicly exposed by most iCIMS portals)."""
    try:
        resp = requests.get(
            f"{portal_base}/feeds/jobs/search?ss=1",
            timeout=TIMEOUT
        )
        if resp.status_code != 200:
            return []
        text   = resp.text
        titles = re.findall(r"<title><!\[CDATA\[(.*?)\]\]></title>", text)
        links  = re.findall(r"<link>(https?://[^\s<]+)</link>", text)
        locs   = re.findall(r"<location>(.*?)</location>", text, re.IGNORECASE)
        jobs = []
        for i, title in enumerate(titles):
            loc  = locs[i]  if i < len(locs)  else ""
            link = links[i] if i < len(links) else portal_base
            if is_match(title, loc):
                jid = re.search(r"/jobs/(\d+)", link)
                jobs.append({
                    "id":       f"icims-{company_name}-{jid.group(1) if jid else i}",
                    "title":    title,
                    "company":  company_name,
                    "location": loc,
                    "url":      link,
                    "source":   "iCIMS",
                })
        return jobs
    except Exception:
        return []

# ── PAYLOCITY ─────────────────────────────────────────────────────────────────
def fetch_paylocity(company_id, company_name):
    """Query Paylocity's public recruiting API."""
    try:
        resp = requests.get(
            f"https://recruiting.paylocity.com/recruiting/v2/api/jobs"
            f"?companyId={company_id}&count=100",
            timeout=TIMEOUT
        )
        data = resp.json()
        jobs = []
        for j in data.get("data", []):
            title  = j.get("jobTitle", "") or j.get("title", "")
            loc    = j.get("location", "") or j.get("city", "")
            job_id = str(j.get("jobId", "") or j.get("id", ""))
            if is_match(title, loc):
                jobs.append({
                    "id":       f"plcty-{company_id}-{job_id}",
                    "title":    title,
                    "company":  company_name,
                    "location": loc,
                    "url":      (f"https://recruiting.paylocity.com/recruiting/jobs/"
                                 f"All/{company_id}/{company_name}/{job_id}"),
                    "source":   "Paylocity",                })
        return jobs
    except Exception:
        return []

# ── ADP ───────────────────────────────────────────────────────────────────────
def fetch_adp(company_id, company_name):
    """Query ADP Workforce Now public recruiting portal."""
    try:
        json_url = (
            f"https://workforcenow.adp.com/mascsr/default/mdf/recruitment/"
            f"getJobSearchResults.cgi?cid={company_id}&ccId=0&type=MP&lang=en_US"
        )
        resp = requests.get(
            json_url,
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
            timeout=TIMEOUT
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs = []
        for j in data.get("jobSearchResults", []):
            title  = j.get("jobTitleDisplay", "") or j.get("jobTitle", "")
            loc    = j.get("jobLocation", "") or j.get("location", "")
            job_id = str(j.get("jobId", "") or j.get("requisitionId", ""))
            if is_match(title, loc):
                jobs.append({
                    "id":       f"adp-{company_id}-{job_id}",
                    "title":    title,
                    "company":  company_name,
                    "location": loc,
                    "url":      (
                        f"https://workforcenow.adp.com/mascsr/default/mdf/recruitment/"
                        f"recruitment.html?cid={company_id}&ccId=0&jobId={job_id}&type=MP&lang=en_US"
                    ),
                    "source":   "ADP",
                })
        return jobs
    except Exception:
        return []

# ── SMARTRECRUITERS ───────────────────────────────────────────────────────────
def fetch_sr(company_id, company_name):
    """SmartRecruiters public job widget API."""
    try:
        resp = requests.get(
            f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings"
            f"?limit=100&status=PUBLISHED",
            timeout=TIMEOUT
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs = []
        for j in data.get("content", []):
            title = j.get("name", "")
            loc   = j.get("location", {}).get("city", "") or ""
            country = j.get("location", {}).get("country", "") or ""
            if country and country.upper() != "US":
                continue
            jid = j.get("id", "")
            ref = j.get("refNumber", jid)
            if is_match(title, loc):
                jobs.append({
                    "id":       f"sr-{company_id}-{jid}",
                    "title":    title,                    "company":  company_name,
                    "location": loc,
                    "url":      f"https://jobs.smartrecruiters.com/{company_id}/{ref}",
                    "source":   "SmartRecruiters",
                })
        return jobs
    except Exception:
        return []

# ── TALEO (Oracle) ────────────────────────────────────────────────────────────
def fetch_taleo(subdomain, company_name):
    """Oracle Taleo public job search API."""
    try:
        resp = requests.get(
            f"https://{subdomain}.taleo.net/careersection/rest/jobboard/posting/search"
            f"?lang=en&start=0&stop=100&radiusUnit=km",
            headers={"Accept": "application/json"},
            timeout=TIMEOUT
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs = []
        for j in data.get("req", {}).get("req", []):
            title   = j.get("req_title_1", j.get("TITLE", ""))
            loc     = j.get("req_location_1", j.get("LOCATION", ""))
            req_no  = j.get("req_no_1", j.get("requisitionno", ""))
            if is_match(title, loc):
                jobs.append({
                    "id":       f"taleo-{subdomain}-{req_no}",
                    "title":    title,
                    "company":  company_name,
                    "location": loc,
                    "url":      f"https://{subdomain}.taleo.net/careersection/2/jobdetail.ftl?job={req_no}",
                    "source":   "Taleo",
                })
        return jobs
    except Exception:
        return []

# ══════════════════════════════════════════════════════════════════════════════
# COMPANY LISTS
# ══════════════════════════════════════════════════════════════════════════════

GREENHOUSE = [
    "airbnb","stripe","databricks","figma","robinhood","brex","plaid",
    "coinbase","reddit","instacart","doordash","lyft","pinterest","dropbox","twilio",
    "zendesk","hubspot","cloudflare","datadog","elastic","confluent","amplitude",
    "mixpanel","dbtlabs","fivetran","hightouch","mode","sigma","hex","modernhealth",
    "tempus","color","headspace","commure","komodohealth","chime","mercury","ramp",
    "marqeta","blend","wayfair","poshmark","vroom","hims","duolingo","coursera",
    "attentive","klaviyo","iterable","braze","benchling","lattice","rippling","deel",
    "remote","drata","vanta","scaleai","labelbox","snorkelai","patientpoint","veeva",
    "asana","box","okta","zoom","docusign","coupa","sprinklr","benchmarksix",
    "samsara","verkada","toast","dutchie","appcues","segment","mparticle",
]

LEVER = [
    "netflix","github","notion","airtable","webflow","canva","miro","loom",
    "zapier","intercom","contentful","heap","fullstory","pendo","productboard",
    "hotjar","surveymonkey","qualtrics","wealthsimple","nerdwallet","avant","arcadia",
    "metabase","preset","lightdash","airwallex","typeform","squarespace",
    "shutterstock","invision","lucid","whimsical","coda","carta","pilot",
    "gusto","ripple","benchmarkemail","gong","outreach","salesloft",
]

ASHBY = [
    "dbt-labs","airbyte","elementary-data","metaplane","datafold","turntable",
    "steep","chalk","cohere","together-ai","anyscale","weights-biases","gretel",
    "scale-ai","linear","retool","vercel","posthog","mercury","ramp","brex","puzzle",
    "middesk","check","gusto","remote","deel","rippling","headway","alma","brightline",
    "nourish","lyra-health","spring-health","cerebral","beehiiv","read-ai","reforge","luma",
    "watershed","patch","arcadia-power","watershed-climate",
]

# Workday — (tenant, job_board_name)
WORKDAY = [
    ("salesforce",       "External_Career_Site"),
    ("adobe",            "External"),
    ("walmart",          "External"),
    ("target",           "Target"),
    ("nvidia",           "External"),
    ("qualcomm",         "External"),
    ("intuit",           "External"),
    ("paypal",           "External"),
    ("ebay",             "External"),
    ("starbucks",        "External"),
    ("cisco",            "External"),
    ("paloaltonetworks", "External"),
    ("gm",               "External"),
    ("ford",             "External"),
    ("nike",             "External"),
    ("gap",              "External"),
    ("expedia",          "External"),
    ("tripadvisor",      "External"),
    ("zillow",           "External"),
    ("etsy",             "External"),
    ("chewy",            "External"),
    ("wayfair",          "External"),
    ("mcdonalds",        "External"),
    ("verizon",          "External"),
    ("att",              "External"),
    ("t-mobile",         "External"),
    # Healthcare / pharma
    ("pfizer",           "External"),
    ("jnj",              "External"),
    ("abbvie",           "External"),
    ("biogen",           "External"),
    ("regeneron",        "External"),
    ("illumina",         "External"),
    ("amgen",            "External"),
    ("medtronic",        "External"),
    ("danaher",          "External"),
    ("abbott",           "External"),
    ("baxter",           "External"),
    ("hologic",          "External"),
    ("bd",               "External"),
    ("zimmer",           "External"),
    # Financial services
    ("capitalone",       "External"),
    ("progressive",      "External"),
    ("allstate",         "External"),
    ("ameriprise",       "External"),
    ("humana",           "External"),
    ("cvs",              "External"),
    ("walgreens",        "External"),
    ("mckesson",         "External"),
    ("cigna",            "External"),
    ("aetna",            "External"),
    ("anthem",           "External"),
    # Enterprise / other
    ("3m",               "External"),
    ("servicenow",       "External"),
    ("vmware",           "External"),
    ("adp",              "External"),
    ("workday",          "External"),
    ("cargill",          "External"),
    ("hp",               "External"),
    ("dell",             "External"),
    ("oracle",           "External"),
    ("sap",              "External"),
]

# Breezy HR — company subdomains ({slug}.breezy.hr)
BREEZY = [
    "datavant","cityblock","life360","podium","gong-io","outreach",
    "salesloft","crossbeam","partnerstack","terminus","bombora",
    "6sense","demandbase","sendoso","g2","trustradius",
    "storyblok","prismic","sanity-io","contentstack",
    "shipbob","shipmonk","project44","transfix",
    "arcadia","brightside","healthjoy","stellar-health",
    "teachable","thinkific","kajabi","podia",
    "sprig","maze","userleap",
    "quantcast","lotame",
    "clearbit","lusha","zoominfo-technologies","apollo-io",
    "mixrank","similarweb","semrush",
]

# Workable — company slugs at apply.workable.com
WORKABLE = [
    "typeform","hotjar","productboard","pendo",
    "logrocket","contentsquare","smartlook",
    "statsig","growthbook","eppo","absmartly",
    "dataiku","rapidminer","knime","datarobot",
    "domo","sisense","yellowfinbi",
    "benchmarkemail","mailerlite","moosend",
    "taxdome","dext","tipalti",
    "workvivo","staffbase","simpplr",
    "gympass","wellhub","limeade",
    "envoy","robin","officespace",
    "factorial","personio","hibob","kenjo",
    "leapsome","reflektive","betterworks",
    "paddle","chargebee","recurly","zuora",
    "adyen","checkout-com","stripe-workable",
]

# iCIMS — (portal_base_url, company_name)
ICIMS = [
    ("https://jobs-pfizer.icims.com",             "Pfizer"),
    ("https://careers-abbvie.icims.com",           "AbbVie"),
    ("https://jobs-lilly.icims.com",               "EliLilly"),
    ("https://jobs-merck.icims.com",               "Merck"),
    ("https://jobs-bms.icims.com",                 "BristolMyersSquibb"),
    ("https://careers-astrazeneca.icims.com",      "AstraZeneca"),
    ("https://jobs-gsk.icims.com",                 "GSK"),
    ("https://jobs-novartis.icims.com",            "Novartis"),
    ("https://careers-ups.icims.com",              "UPS"),
    ("https://jobs-fedex.icims.com",               "FedEx"),
    ("https://careers-lockheedmartin.icims.com",   "LockheedMartin"),
    ("https://jobs-boeing.icims.com",              "Boeing"),
    ("https://careers-northropgrumman.icims.com",  "NorthropGrumman"),
    ("https://jobs-raytheon.icims.com",            "Raytheon"),
    ("https://careers-cigna.icims.com",            "Cigna"),
    ("https://jobs-aig.icims.com",                 "AIG"),
    ("https://careers-metlife.icims.com",          "MetLife"),
    ("https://jobs-prudential.icims.com",          "Prudential"),
    ("https://careers-schwab.icims.com",           "CharlesSchwab"),
    ("https://jobs-fidelity.icims.com",            "Fidelity"),
    ("https://careers-vanguard.icims.com",         "Vanguard"),
    ("https://jobs-pnc.icims.com",                 "PNC"),
    ("https://careers-usbank.icims.com",           "USBank"),
    ("https://careers-publicis.icims.com",         "Publicis"),
    ("https://jobs-omnicom.icims.com",             "Omnicom"),
]

# Paylocity — (company_id, display_name)
PAYLOCITY = [
    ("24002", "Cvent"),
    ("19765", "Daxko"),
    ("30948", "HireVue"),
    ("14831", "HealthStream"),
    ("21012", "AssuredPartners"),
    ("22105", "Vivint"),
    ("27885", "ImagineLeaming"),
    ("13405", "FranklinCovey"),
    ("24891", "MedBridge"),
    ("29874", "Ceribell"),
    ("23440", "CaliberImaging"),
    ("26108", "Brightree"),
    ("15562", "Apogee"),
    ("17023", "Acuity"),
    ("28190", "SambaSafety"),
]

# ADP — (company_id, display_name)
ADP = [
    ("6007261009419", "Macys"),
    ("5000501859812", "Hertz"),
    ("5000231919001", "UniversalHealthServices"),
    ("5000551887502", "ManpowerGroup"),
    ("6000016581610", "Conduent"),
    ("5000645513801", "NCR"),
    ("5000474257501", "Aramark"),
    ("5000394717601", "IQVIA"),
    ("5000571732901", "SyneosHealth"),
    ("5000387718601", "Parexel"),
    ("5000467989301", "IconPLC"),
    ("5000556082601", "CharlesRiver"),
    ("5000422396901", "Labcorp"),
]

# SmartRecruiters — (company_id_slug, display_name)
SMARTRECRUITERS = [
    ("ALDI", "ALDI"),
    ("Ikea", "IKEA"),
    ("Bosch", "Bosch"),
    ("Siemens", "Siemens"),
    ("McDonalds", "McDonalds"),
    ("Visa", "Visa"),
    ("Mastercard", "Mastercard"),
    ("LinkedIn", "LinkedIn"),
    ("Twitter", "Twitter/X"),
    ("Snap", "Snap"),
    ("Twitch", "Twitch"),
    ("Spotify", "Spotify"),
    ("Klarna", "Klarna"),
    ("Zalando", "Zalando"),
    ("Delivery Hero", "DeliveryHero"),
    ("N26", "N26"),
    ("Celonis", "Celonis"),
    ("Personio", "Personio-SR"),
    ("GetYourGuide", "GetYourGuide"),
    ("HelloFresh", "HelloFresh"),
]

# Taleo — (subdomain, display_name)
TALEO = [
    ("gm",           "GeneralMotors"),
    ("boeing",       "Boeing"),
    ("lockheed",     "LockheedMartin"),
    ("raytheon",     "RaytheonTechnologies"),
    ("ge",           "GeneralElectric"),
    ("honeywell",    "Honeywell"),
    ("ups",          "UPS"),
    ("fedex",        "FedEx"),
    ("target",       "Target"),
    ("bestbuy",      "BestBuy"),
    ("kroger",       "Kroger"),
    ("costco",       "Costco"),
    ("walgreens",    "Walgreens"),
    ("cvs",          "CVSHealth"),
    ("humana",       "Humana"),
    ("unitedhealth", "UnitedHealth"),
    ("anthem",       "Anthem"),
    ("cigna",        "CignaTaleo"),
    ("jpmorgan",     "JPMorganChase"),
    ("bankofamerica","BankofAmerica"),
    ("wellsfargo",   "WellsFargo"),
    ("goldman",      "GoldmanSachs"),
    ("morganstanley","MorganStanley"),
    ("blackrock",    "BlackRock"),
    ("fidelity",     "FidelityTaleo"),
]

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run():
    seen = load_cache()

    # Build task list: each item is (fn, *args)
    tasks = (
        [(fetch_gh,        c)    for c    in GREENHOUSE]
      + [(fetch_lv,        c)    for c    in LEVER]
      + [(fetch_ab,        c)    for c    in ASHBY]
      + [(fetch_wd,        t, b) for t, b in WORKDAY]
      + [(fetch_breezy,    s)    for s    in BREEZY]
      + [(fetch_workable,  s)    for s    in WORKABLE]
      + [(fetch_icims,     p, n) for p, n in ICIMS]
      + [(fetch_paylocity, i, n) for i, n in PAYLOCITY]
      + [(fetch_adp,       i, n) for i, n in ADP]
      + [(fetch_sr,        i, n) for i, n in SMARTRECRUITERS]
      + [(fetch_taleo,     s, n) for s, n in TALEO]
    )

    all_jobs = []
    platform_counts = {}   # source -> total matching jobs found
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(item[0], *item[1:]): item for item in tasks}
        for f in as_completed(futs):
            jobs = f.result()
            all_jobs.extend(jobs)
            for j in jobs:
                src = j["source"]
                platform_counts[src] = platform_counts.get(src, 0) + 1

    # Per-platform summary (always visible in Actions log)
    print("\n── Platform scan results ──────────────────────────────────")
    for platform in ["Greenhouse","Lever","Ashby","Workday",
                     "BreezyHR","Workable","iCIMS","Paylocity","ADP",
                     "SmartRecruiters","Taleo"]:
        n = platform_counts.get(platform, 0)
        print(f"  {platform:<16}: {n:>4} matching USA jobs")
    print(f"  {'TOTAL':<16}: {len(all_jobs):>4} matching USA jobs")
    print("────────────────────────────────────────────────────────────\n")
    # Deduplicate and keep only net-new
    seen_ids, new_jobs = set(), []
    for j in all_jobs:
        if j["id"] not in seen and j["id"] not in seen_ids:
            seen_ids.add(j["id"])
            new_jobs.append(j)

    # Sort by source then title for readability
    new_jobs.sort(key=lambda j: (j["source"], j["title"]))

    print(f"Scanned: {len(all_jobs)} total | New (not in cache): {len(new_jobs)}")

    if new_jobs:
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        lines = [
            f"# {len(new_jobs)} New DS/Analytics Jobs — USA Only — {ts}", "",
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

    # Update cache (mark everything seen this scan)
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
