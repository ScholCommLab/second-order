# coding: utf-8
import datetime
import requests
import pandas as pd

from tqdm import tqdm

from concurrent.futures import ProcessPoolExecutor
from requests_futures.sessions import FuturesSession

# Read input file
df = pd.read_csv("shortened_urls.txt")
urls = df.short_url.tolist()

# Options
batchsize = 100
max_workers = 100
timeout = 5

# Init output df
resolved_urls = pd.DataFrame({'short_url': urls,
                              'url': None,
                              'ts': None,
                              'err': None,
                              'err_msg': None,
                              'status_code': None})
resolved_urls = resolved_urls.set_index("short_url")

# Split dois into batches
batches = range(0, len(urls), batchsize)

# FutureSession
session = FuturesSession(max_workers=max_workers)
for i in tqdm(batches, total=len(batches)):
    futures = []
    batch = urls[i:i + batchsize]

    # create futures in parallel
    for short_url in batch:
        now = datetime.datetime.now()
        future = session.get(short_url,
                             allow_redirects=True,
                             timeout=timeout)

        futures.append({
            "short_url": short_url,
            "ts": str(now),
            "future": future
        })

    # collect future respones and populate df
    for response in futures:
        resolved_urls.loc[response['short_url'], 'ts'] = response['ts']
        err = None
        err_msg = None
        status = None

        try:
            resolved_urls.loc[response['short_url'],
                              'url'] = response['future'].result().url
            resolved_urls.loc[response['short_url'],
                              'status_code'] = response['future'].result().status_code
        except requests.exceptions.Timeout as ex:
            err_msg = str(ex)
            err = "Timeout"
        except requests.exceptions.TooManyRedirects as ex:
            err_msg = str(ex)
            err = "TooManyRedirects"
        except requests.exceptions.RequestException as ex:
            err_msg = str(ex)
            err = "RequestException"

        resolved_urls.loc[response['short_url'], 'err'] = err
        resolved_urls.loc[response['short_url'], 'err_msg'] = err_msg

resolved_urls.to_csv("resolved_urls.csv")
