import os,re, json, hashlib, boto3 
from typing import List, Dict
from pinecone import Pinecone, ServerlessSpec 
from dotenv import load_dotenv 
from helper.config_loader import load_config
load_dotenv()

# env 
# bucket=os.getenv('')
# s3_prefix=os.getenv('')
# titan_model_id=os.getenv('')

# pipnecone_api_key=os.getenv('')
# pinecone_index=os.getenv('')
# pinecone_cloud=

def handler(event,context):
    config=load_config()
    S3_PREFIX=config['S3_PREFIX']
    records=event.get('Records',[])
    results=[]
    for r in records:
        if 's3' not in r:
            continue 
        b=r['s3']['bucket']['name']
        k=r['s3']['object']['key']
        if S3_PREFIX and not k.startswith(S3_PREFIX):
            continue 
        results.append(b,k)
    return {'ok':True,'results':results}

