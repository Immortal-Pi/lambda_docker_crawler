# lambda function handler 
import numpy as np

def handler(event, context):
    arr=np.random.randint(0,10,(3,3))
    return {"satusCode":200, 
            "body":{"array":arr.tolist()}}