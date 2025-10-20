# lambda function handler 
# from datetime import datetime, timezone
# import hashlib, boto3, asyncio
# from crawl4ai import AsyncWebCrawler
# import os 
# from helper.config_loader import load_config
# # lambda function handler
# from datetime import datetime, timezone
# import hashlib, boto3, asyncio, os
# from crawl4ai import AsyncWebCrawler
# from helper.config_loader import load_config

# _s3 = boto3.client("s3")
# _config = None  # lazy-load once per container

# def _get_config():
#     global _config
#     if _config is None:
#         # allow override via env in case file is missing
#         _config = load_config()
#         _config.setdefault("S3_BUCKET", os.getenv("S3_BUCKET", "utd-catalog-tokenaughts"))
#         _config.setdefault("SEED_URL", os.getenv("SEED_URL", "https://catalog.utdallas.edu/2025/graduate/courses"))
#     return _config

# def _make_s3_key(prefix="catalog"):
#     now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
#     year = datetime.now(timezone.utc).strftime("%Y")
#     return f"{prefix}/{year}/all_{now}.md"  # markdown extension

# async def _crawl(seed: str) -> str:
#     # Browserless, with some resilience
#     headers = {
#         "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
#                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
#         "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#     }
#     async with AsyncWebCrawler(verbose=False, browserless=True, headers=headers, timeout=20) as crawler:
#         r = await crawler.arun(
#             url=seed,
#             exclude_external_links=True,
#             process_iframes=False,
#             remove_overlay_elements=True,
#         )
#         return r.markdown if (r and r.success and r.markdown) else ""

# def handler(event, context):
#     cfg = _get_config()
#     s3_bucket = cfg["S3_BUCKET"]
#     seed_url  = cfg["SEED_URL"]

#     text = asyncio.run(_crawl(seed_url))
#     if not text or len(text.strip()) < 50:
#         # Return a helpful reason for observability
#         return {"wrote": False, "reason": "crawl returned empty/short", "seed": seed_url}

#     sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
#     s3_key = _make_s3_key()

#     _s3.put_object(
#         Bucket=s3_bucket,
#         Key=s3_key,
#         Body=text.encode("utf-8"),
#         Metadata={"sha256": sha, "seed": seed_url},
#         ContentType="text/markdown; charset=utf-8",
#     )
#     return {"bucket": s3_bucket, "key": s3_key, "sha256": sha, "bytes": len(text)}

# def handler(event, context):
#     arr=np.random.randint(0,10,(3,3))
#     return {"satusCode":200, 
#             "body":{"array":arr.tolist()}}

# lambda function handler 
# Lambda A — extract ALL course pages → single S3 file (browserless)
from datetime import datetime, timezone
import asyncio, os, hashlib
import boto3
from urllib.parse import urlparse
from crawl4ai import AsyncWebCrawler
from helper.config_loader import load_config
config=load_config()

_s3 = boto3.client("s3")

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

    async with AsyncWebCrawler(verbose=False, browserless=True, headers=headers, timeout=25) as crawler:
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

            # polite delay (non-blocking)
            await asyncio.sleep(RATE_SLEEP)

    # one big markdown
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

