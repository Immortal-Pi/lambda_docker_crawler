# lambda_b_embed.py
import os, json, uuid, hashlib, psycopg2, boto3

AWS_REGION  = os.environ["AWS_REGION"]
S3_BUCKET   = os.environ["S3_BUCKET"]
S3_KEY      = os.environ["S3_KEY"]
EMBED_DIM   = int(os.getenv("EMBED_DIM","1024"))
EMB_MODEL   = os.environ["BEDROCK_EMBED_MODEL"]

DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = int(os.getenv("DB_PORT","5432"))
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)

def db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD, sslmode="require"
    )

def chunk(text:str, size=2000, overlap=200):
    i=0; n=len(text)
    while i<n:
        j=min(n, i+size)
        yield text[i:j]
        i = j - overlap if j<n else j
        if i<0: i=0

def embed(texts):
    body = {"inputText": texts if isinstance(texts,list) else [texts]}
    resp = bedrock.invoke_model(modelId=EMB_MODEL,
                                body=json.dumps(body),
                                accept="application/json",
                                contentType="application/json")
    payload = json.loads(resp["body"].read())
    vecs = payload.get("embeddings") or payload.get("embedding")
    return vecs if isinstance(vecs[0], list) else [vecs]

def ensure_schema():
    sql = f"""
    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE TABLE IF NOT EXISTS catalog_doc (
      doc_id UUID PRIMARY KEY, s3_key TEXT UNIQUE, sha256 TEXT, created_at TIMESTAMPTZ DEFAULT now());
    CREATE TABLE IF NOT EXISTS catalog_chunk (
      chunk_id UUID PRIMARY KEY, doc_id UUID REFERENCES catalog_doc(doc_id) ON DELETE CASCADE,
      chunk_index INT, text_clean TEXT, embedding VECTOR({EMBED_DIM}), created_at TIMESTAMPTZ DEFAULT now());
    CREATE INDEX IF NOT EXISTS catalog_chunk_embedding_idx
      ON catalog_chunk USING ivfflat (embedding vector_cosine_ops) WITH (lists = 200);
    """
    with db() as conn, conn.cursor() as cur: cur.execute(sql)

def upsert_doc(s3_key, sha):
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
          INSERT INTO catalog_doc (doc_id, s3_key, sha256)
          VALUES (%s, %s, %s)
          ON CONFLICT (s3_key) DO UPDATE SET sha256 = EXCLUDED.sha256
          RETURNING doc_id;
        """, (str(uuid.uuid4()), s3_key, sha))
        return cur.fetchone()[0]

def handler(event, context):
    # S3 trigger OR scheduled: always read the configured object
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    text = obj["Body"].read().decode("utf-8")
    meta = obj.get("Metadata", {})
    sha = meta.get("sha256") or hashlib.sha256(text.encode("utf-8")).hexdigest()

    ensure_schema()

    # Idempotency: check if same sha already stored
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT doc_id FROM catalog_doc WHERE s3_key=%s AND sha256=%s", (S3_KEY, sha))
        row = cur.fetchone()
        if row:
            return {"skipped": True, "reason": "same hash already processed"}

    doc_id = upsert_doc(S3_KEY, sha)

    # remove old chunks for this doc (simple, safe)
    with db() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM catalog_chunk WHERE doc_id=%s", (doc_id,))

    # chunk → embed → insert (simple loop)
    idx = 0
    for ctext in chunk(text):
        vec = embed([ctext])[0]
        with db() as conn, conn.cursor() as cur:
            cur.execute("""
              INSERT INTO catalog_chunk (chunk_id, doc_id, chunk_index, text_clean, embedding)
              VALUES (%s, %s, %s, %s, %s)
            """, (str(uuid.uuid4()), doc_id, idx, ctext, vec))
        idx += 1

    return {"processed": True, "doc_id": doc_id, "chunks": idx}