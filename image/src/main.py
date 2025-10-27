import os 
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/ms-playwright"
os.environ["HOME"] = "/tmp"
os.environ["XDG_CACHE_HOME"] = "/tmp/.cache"
os.environ["XDG_DATA_HOME"]  = "/tmp/.local/share"
os.environ["XDG_STATE_HOME"] = "/tmp/.local/state"

# If crawl4ai honors a custom cache/db dir, set them (harmless if unused)
os.environ["CRAWL4AI_CACHE_DIR"] = "/tmp/crawl4ai/cache"
os.environ["CRAWL4AI_DB_PATH"]   = "/tmp/crawl4ai/db"
from datetime import datetime, timezone
import asyncio, os, hashlib, pathlib
import boto3
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler
from helper.config_loader import load_config
config=load_config()

_s3 = boto3.client("s3")


# If you use Playwright at runtime, keep its caches in /tmp too
# os.environ["PLAYWRIGHT_BROWSERS_PATH"] = "/opt/ms-playwright"

# Pre-create dirs to avoid races
for p in [
    "/tmp/.cache", "/tmp/.local/share", "/tmp/.local/state",
    "/tmp/crawl4ai/cache", "/tmp/crawl4ai/db",
]:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)
LAUNCH_ARGS = {
    "headless": True,
    "args": [
        "--headless=new",
         "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-webgl",
        "--disable-accelerated-2d-canvas",
        "--use-gl=swiftshader",         # <- force software GL, avoids GPU proc
        "--use-angle=swiftshader",      # <- belt & suspenders
        "--no-zygote",
        "--disable-features=IsolateOrigins,site-per-process",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
    ],
}
# -------- config helpers --------
def _env(k, default=None):
    v = os.getenv(k)
    return v if v is not None else default

S3_BUCKET = config['S3_BUCKET']
SEED_URL  = config['SEED_URL'] 
INCLUDE_FRAGMENT = config['INCLUDE_FRAGMENT']
RATE_SLEEP = config['RATE_SLEEP']     # polite delay between requests
MIN_LEN = config['MIN_LEN']              # skip very short pages
MAX_PAGES = config['MAX_PAGES']           # 0 = no cap; use for safety if you want

def _make_s3_key(prefix="catalog"):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    year = datetime.now(timezone.utc).strftime("%Y")
    return f"{prefix}/{year}/all_{now}.md"

def _same_site(url, seed):
    a, b = urlparse(url), urlparse(seed)
    return (a.scheme, a.netloc) == (b.scheme, b.netloc)

def _header(url: str, title: str | None) -> str:
    t = (title or "").strip()
    h = f"\n\n### SOURCE: {url}\n"
    return h + (f"#### {t}\n\n" if t else "\n")

# -------- crawler --------
async def _fetch(crawler: AsyncWebCrawler, url: str):
    r = await crawler.arun(
        url=url,
        exclude_external_links=True,
        process_iframes=False,
        remove_overlay_elements=True,
        bypass_cache=False,
    )
    if r and r.success and r.markdown and len(r.markdown) >= MIN_LEN:
        return r
    return None


async def _crawl_all(seed: str) -> str:
    headers = {
        "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    blocks = []
    count = 0

    # ⬇️ CHANGED: browserless=False (local), pass browser_args, limit concurrency, longer timeout
    async with AsyncWebCrawler(
        browser_type="chromium",
    launch_options=LAUNCH_ARGS,   # <-- IMPORTANT
    max_concurrency=1,
    timeout=90,
    navigation_timeout=60000,        # if your version supports it
    page_timeout=60000,              # if supported
    headers=headers,
    verbose=False,
    ) as crawler:

        # 1) hub
        hub = await _fetch(crawler, seed)
        if not hub:
            return ""  # bail: nothing to write
        blocks.append(_header(seed, hub.metadata.get("title")))
        blocks.append(hub.markdown)
        blocks.append("\n---\n")
        count += 1

        # 2) discover internal links on hub
        links = []
        for link in hub.links.get("internal", []):
            href = link.get("href")
            if not href:
                continue
            if INCLUDE_FRAGMENT in href and _same_site(href, seed):
                links.append(href)

        # 3) crawl each subject/course page (dedup; optional cap)
        seen = set()
        for url in links:
            if url in seen:
                continue
            seen.add(url)
            if MAX_PAGES and count >= MAX_PAGES:
                break

            r = await _fetch(crawler, url)
            if r:
                blocks.append(_header(url, r.metadata.get("title")))
                blocks.append(r.markdown)
                blocks.append("\n---\n")
                count += 1

            await asyncio.sleep(RATE_SLEEP)  # polite delay

    return "".join(blocks)

# -------- lambda handler --------
def handler(event, context):
    text = asyncio.run(_crawl_all(SEED_URL))
    if not text or len(text.strip()) < MIN_LEN:
        return {"wrote": False, "reason": "crawl returned empty/short", "seed": SEED_URL}

    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    s3_key = _make_s3_key()

    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=text.encode("utf-8"),
        Metadata={"sha256": sha, "seed": SEED_URL},
        ContentType="text/markdown; charset=utf-8",
    )
    return {"bucket": S3_BUCKET, "key": s3_key, "sha256": sha, "bytes": len(text)}


# def handler(event, context):
#     arr=np.random.randint(0,10,(3,3))
#     return {"satusCode":200, 
#             "body":{"array":arr.tolist()}}

