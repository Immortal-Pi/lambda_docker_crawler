# minimal deps: httpx, selectolax, html2text, boto3
# pip install httpx selectolax html2text boto3

import asyncio, re, hashlib, os, pathlib
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urldefrag
import httpx
from selectolax.parser import HTMLParser
import html2text
import boto3
from helper.clean_data import clean_text
# -------- config (reuse your keys/values) --------
S3_BUCKET         = os.environ.get("S3_BUCKET", "utd-catalog-tokenaughts")
SEED_URL          = os.environ.get("SEED_URL", "https://catalog.utdallas.edu/2025/graduate/courses")
INCLUDE_FRAGMENT  = os.environ.get("INCLUDE_FRAGMENT", "/courses/")    # keep only links that contain this
RATE_SLEEP        = float(os.environ.get("RATE_SLEEP", "0.25"))
MIN_LEN           = int(os.environ.get("MIN_LEN", "300"))
MAX_PAGES         = int(os.environ.get("MAX_PAGES", "0"))              # 0 = no cap

TIMEOUT_SEC       = 20
CONCURRENCY       = 8                                                  # small, polite
HEADERS = {
    "user-agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124 Safari/537.36"),
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_s3 = boto3.client("s3")
_md = html2text.HTML2Text()
_md.ignore_links = False
_md.body_width = 0

def _same_site(u, seed):
    a, b = urlparse(u), urlparse(seed)
    return (a.scheme, a.netloc) == (b.scheme, b.netloc)

def _normalize(seed, href):
    if not href:
        return None
    href, _ = urldefrag(href)              # drop #fragments
    url = urljoin(seed, href)
    # only http/https
    if not url.startswith(("http://", "https://")):
        return None
    return url

def _make_s3_key(prefix="catalog"):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    year = datetime.now(timezone.utc).strftime("%Y")
    return f"{prefix}/{year}/all_{now}.md"

def _header(url: str, title: str | None) -> str:
    t = (title or "").strip()
    h = f"\n\n### SOURCE: {url}\n"
    return h + (f"#### {t}\n\n" if t else "\n")

async def fetch_html(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        r = await client.get(url, headers=HEADERS, timeout=TIMEOUT_SEC, follow_redirects=True)
        if r.status_code >= 400 or "text/html" not in r.headers.get("content-type", ""):
            return None
        return r.text
    except Exception:
        return None

def extract_title_and_links(seed: str, html: str) -> tuple[str | None, list[str]]:
    doc = HTMLParser(html)
    title = None
    # try standard <title>
    tnode = doc.css_first("title")
    if tnode and tnode.text():
        title = tnode.text().strip()
    # collect all internal links
    links = []
    for a in doc.css("a[href]"):
        href = a.attributes.get("href")
        url = _normalize(seed, href)
        if not url:
            continue
        if _same_site(url, seed) and (INCLUDE_FRAGMENT in url if INCLUDE_FRAGMENT else True):
            links.append(url)
    return title, links

def html_to_markdown(html: str) -> str:
    # Try to focus on main content area if present
    doc = HTMLParser(html)
    main = doc.css_first("main") or doc.css_first("#content") or doc.css_first(".content") or doc.body
    if not main:
        return ""
    # Convert only that subtree to HTML string, then to Markdown
    subtree_html = main.html if hasattr(main, "html") else str(main)
    md = _md.handle(subtree_html or "")
    # light cleanup
    md = re.sub(r"\n{3,}", "\n\n", md).strip()
    return md

async def crawl_catalog(seed: str) -> str:
    blocks = []
    seen  = set()
    queue = []

    limits = httpx.Limits(max_connections=CONCURRENCY, max_keepalive_connections=CONCURRENCY)
    async with httpx.AsyncClient(http2=True, timeout=TIMEOUT_SEC, limits=limits) as client:
        # Fetch hub
        html = await fetch_html(client, seed)
        if not html:
            return ""
        title, links = extract_title_and_links(seed, html)
        md = html_to_markdown(html)
        if md and len(md) >= MIN_LEN:
            blocks += [_header(seed, title), md, "\n---\n"]
        # seed queue
        for u in links:
            if u not in seen:
                seen.add(u)
                queue.append(u)

        # BFS over subject/course pages
        count = 1
        sem = asyncio.Semaphore(CONCURRENCY)

        async def worker(url: str):
            nonlocal count
            async with sem:
                h = await fetch_html(client, url)
                await asyncio.sleep(RATE_SLEEP)
            if not h:
                return None
            t, _ = extract_title_and_links(seed, h)
            m = html_to_markdown(h)
            if m and len(m) >= MIN_LEN:
                blocks.extend([_header(url, t), m, "\n---\n"])
                count += 1
            return None

        tasks = []
        for url in queue:
            if MAX_PAGES and count >= MAX_PAGES:
                break
            tasks.append(asyncio.create_task(worker(url)))
        if tasks:
            await asyncio.gather(*tasks)

    return "".join(blocks)

# ---------- Lambda handler ----------
def handler(event, context):
    # Ensure /tmp exists (Lambda)
    pathlib.Path("/tmp").mkdir(exist_ok=True)
    text = asyncio.run(crawl_catalog(SEED_URL))
    if not text or len(text.strip()) < MIN_LEN:
        return {"wrote": False, "reason": "empty/short", "seed": SEED_URL}
    raw_text=clean_text(text)
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    s3_key = _make_s3_key()
    _s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=raw_text.encode("utf-8"),
        Metadata={"sha256": sha, "seed": SEED_URL},
        ContentType="text/markdown; charset=utf-8",
    )
    return {"bucket": S3_BUCKET, "key": s3_key, "sha256": sha, "bytes": len(text)}