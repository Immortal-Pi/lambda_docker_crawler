# lambda function handler 
from datetime import datetime, timezone
import hashlib, boto3, asyncio
from crawl4ai import AsyncWebCrawler
import os 
from helper.config_loader import load_config
config=load_config()

S3_BUCKET = config["S3_BUCKET"]
SEED_URL  = config["SEED_URL"]
s3 = boto3.client("s3")

def make_s3_key():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    # put under a logical prefix
    return f"catalog/2025/all_{now}.txt"

async def crawl(seed: str) -> str:
    async with AsyncWebCrawler(verbose=True, browserless=True) as crawler:
        r = await crawler.arun(url=seed, exclude_external_links=True)
        return r.markdown if r.success else ""

def handler(event, context):
    text = asyncio.run(crawl(SEED_URL))
    if not text:
        return {"wrote": False, "reason": "empty text"}
    sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    s3_key = make_s3_key()               # ✅ generate new key
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=text.encode("utf-8"),
        Metadata={"sha256": sha, "seed": SEED_URL},
    )
    return {"bucket": S3_BUCKET, "key": s3_key, "sha256": sha}


# def handler(event, context):
#     arr=np.random.randint(0,10,(3,3))
#     return {"satusCode":200, 
#             "body":{"array":arr.tolist()}}

# import os, json, time, uuid, hashlib, asyncio
# import boto3
# import psycopg2
# import psycopg2.extras
# from crawl4ai import AsyncWebCrawler

# # ---- env
# AWS_REGION  = os.getenv("AWS_REGION","us-east-1")
# S3_BUCKET   = os.getenv("S3_BUCKET","utd-catalog")
# S3_KEY      = os.getenv("S3_KEY","catalog/2025/all.md")
# SEED_URL    = os.getenv("SEED_URL","https://catalog.utdallas.edu/2025/graduate/courses")
# EMB_MODEL   = os.getenv("BEDROCK_EMBED_MODEL","amazon.titan-embed-text-v2:0")
# EMBED_DIM   = int(os.getenv("EMBED_DIM","1024"))

# DB_HOST     = os.environ["DB_HOST"]
# DB_PORT     = int(os.getenv("DB_PORT","5432"))
# DB_NAME     = os.environ["DB_NAME"]
# DB_USER     = os.environ["DB_USER"]
# DB_PASSWORD = os.environ["DB_PASSWORD"]

# s3 = boto3.client("s3")
# bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)

# # ---- helpers
# def sha256(s: str) -> str:
#     return hashlib.sha256(s.encode("utf-8")).hexdigest()

# def chunk_text(text: str, approx_chars=2000, overlap=200):
#     out=[]; n=len(text); i=0
#     while i < n:
#         j = min(n, i+approx_chars)
#         out.append(text[i:j])
#         i = j - overlap if j < n else j
#         if i < 0: i = 0
#     return out

# def db_conn():
#     return psycopg2.connect(
#         host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
#         user=DB_USER, password=DB_PASSWORD,
#         connect_timeout=5, sslmode="require"
#     )

# def ensure_schema():
#     sql = f"""
#     CREATE EXTENSION IF NOT EXISTS vector;

#     CREATE TABLE IF NOT EXISTS catalog_doc (
#       doc_id UUID PRIMARY KEY,
#       url TEXT,
#       title TEXT,
#       html_hash TEXT,
#       created_at TIMESTAMPTZ DEFAULT now()
#     );

#     CREATE TABLE IF NOT EXISTS catalog_chunk (
#       chunk_id UUID PRIMARY KEY,
#       doc_id UUID REFERENCES catalog_doc(doc_id) ON DELETE CASCADE,
#       chunk_index INT,
#       text_clean TEXT,
#       embedding VECTOR({EMBED_DIM}),
#       created_at TIMESTAMPTZ DEFAULT now()
#     );

#     CREATE INDEX IF NOT EXISTS catalog_chunk_embedding_idx
#     ON catalog_chunk
#     USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200);
#     """
#     with db_conn() as conn, conn.cursor() as cur:
#         cur.execute(sql)

# def embed_texts(texts):
#     body = {"inputText": texts if isinstance(texts, list) else [texts]}
#     resp = bedrock.invoke_model(
#         modelId=EMB_MODEL,
#         body=json.dumps(body),
#         accept="application/json",
#         contentType="application/json",
#     )
#     payload = json.loads(resp["body"].read())
#     vectors = payload.get("embeddings") or payload.get("embedding")
#     # normalize to List[List[float]]
#     return vectors if isinstance(vectors[0], list) else [vectors]

# # ---- action 1: crawl -> one big S3 file
# async def crawl_all(seed: str) -> str:
#     pieces = []
#     async with AsyncWebCrawler(verbose=False) as crawler:
#         hub = await crawler.arun(url=seed, exclude_external_links=True, process_iframes=True)
#         if not hub.success:
#             return ""
#         internal = [l["href"] for l in hub.links["internal"] if "/2025/graduate/courses/" in l["href"]]
#         seen = set()
#         for url in internal:
#             if url in seen: 
#                 continue
#             seen.add(url)
#             r = await crawler.arun(url=url, exclude_external_links=True, process_iframes=True)
#             if r.success and r.markdown and len(r.markdown) > 50:
#                 # separate pages with a clear delimiter + include URL as a header
#                 pieces.append(f"# SOURCE: {url}\n\n{r.markdown}\n\n---\n")
#             time.sleep(0.3)
#     return "".join(pieces)

# def do_crawl():
#     text = asyncio.run(crawl_all(SEED_URL))
#     if not text:
#         return {"wrote": False, "reason":"crawl returned empty"}
#     # write single file to S3
#     s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=text.encode("utf-8"),
#                   Metadata={"seed": SEED_URL, "sha256": sha256(text)})
#     return {"wrote": True, "bytes": len(text)}

# # ---- action 2: read S3 -> chunk -> embed -> insert rows (no batch)
# def do_embed():
#     ensure_schema()

#     # read the single big file
#     obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
#     text = obj["Body"].read().decode("utf-8")
#     h = sha256(text)

#     # insert one doc row (doc == this whole file)
#     doc_id = str(uuid.uuid4())
#     with db_conn() as conn, conn.cursor() as cur:
#         cur.execute(
#             "INSERT INTO catalog_doc (doc_id, url, title, html_hash) VALUES (%s,%s,%s,%s)",
#             (doc_id, f"s3://{S3_BUCKET}/{S3_KEY}", "UTD 2025 Catalog (All)", h)
#         )
#         conn.commit()

#     # chunk + embed + insert per chunk (simple loop; no bulk)
#     chunks = chunk_text(text, approx_chars=2000, overlap=200)
#     for i, c in enumerate(chunks):
#         vec = embed_texts([c])[0]        # one call per chunk (simple)
#         with db_conn() as conn, conn.cursor() as cur:
#             cur.execute(
#                 "INSERT INTO catalog_chunk (chunk_id, doc_id, chunk_index, text_clean, embedding) "
#                 "VALUES (%s,%s,%s,%s,%s)",
#                 (str(uuid.uuid4()), doc_id, i, c, vec)
#             )
#             conn.commit()

#     return {"doc_id": doc_id, "chunks": len(chunks)}

# # ---- Lambda entry
# def handler(event, context):
#     action = (event or {}).get("action", "crawl")  # "crawl" | "embed" | "refresh"
#     if action == "crawl":
#         return do_crawl()
#     elif action == "embed":
#         return do_embed()
#     elif action == "refresh":
#         a = do_crawl()
#         b = do_embed()
#         return {"crawl": a, "embed": b}
#     else:
#         return {"message": "unknown action", "input": event}
