import http.client
import json
import random
from ksuid import Ksuid
import hashlib
from tqdm import tqdm
from timeit import timeit

debug = False
url = ""
million = 1000000
itCount = 1*million

def submit_hash(hash) -> bool:
    conn = http.client.HTTPSConnection(url)
    payload = { "Content": hash }
    headers = { 'content-type': "application/json" }
    conn.request("POST", "/dev/submit-hash", json.dumps(payload), headers)
    res = conn.getresponse()
    data = res.read()
    data_str = data.decode("utf-8")
    ok = data_str == "success"
    if (not ok) or debug: print(f"submit {hash}: {data_str}")
    return ok

def run_test():
    for x in tqdm(range(itCount)):
        hash = hashlib.sha256(str(Ksuid()).encode())
        submit_hash(hash.hexdigest())

if __name__ == '__main__':
    from datetime import datetime
    start_time = datetime.now()
    run_test()
    end_time = datetime.now()
    
    print(f"start: {start_time.timestamp()}, end: {end_time.timestamp()}, cost: {start_time.timestamp() - end_time.timestamp()}")
