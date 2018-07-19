import configparser
import csv
import gc
import json
import operator
import os.path
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
import tweepy
from dateutil.parser import parse

try:  # for notebooks
    get_ipython
    from tqdm._tqdm_notebook import tqdm_notebook as tqdm
except:  # for commandline
    from tqdm import tqdm
tqdm.pandas()

Config = configparser.ConfigParser()
Config.read('../config.cnf')

consumer_key = Config.get('twitter_keys', 'consumer_key')
consumer_secret = Config.get('twitter_keys', 'consumer_secret')
access_token = Config.get('twitter_keys', 'access_token')
access_token_secret = Config.get('twitter_keys', 'access_token_secret')

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# set up access to the Twitter API
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


data_dir = Path("../data/")
queries_dir = data_dir / "queries/"
output = data_dir / "refetched/"

row_counts = {}
with open(str(queries_dir / 'row_counts.txt')) as f:
    reader = csv.reader(f, delimiter='\t')
    for row in reader:
        row_counts[row[0]] = int(row[1])

sorted_files = sorted(row_counts.items(), key=operator.itemgetter(1))


def load_json(x):
    try:
        return json.loads(x, strict=False)
    except:
        print("Problematic tweet found.")
        return None

for filename, rows in sorted_files:
    query_file = queries_dir / filename
    if not query_file.exists():
        continue

    filebase = filename.split(".txt")[0]
    outfile = output / (filebase + ".csv")

    headers = ['tweet_id', 'posted_on', 'tweet', 'truncated', 'refetched', 'error', 'retweet_id', 'retweet_truncated']

    skip_header = False
    if outfile.exists():
        skip_header = True

    print("Collecting {}".format(filebase))
    with open(str(query_file), "r") as infile:
        with open(str(outfile), "a") as outfile:
            reader = csv.reader(infile, delimiter='\t')
            writer = csv.writer(outfile)

            if not skip_header:
                writer.writerow(headers)

            for row in tqdm(reader, total=rows):
                tweet_id = row[0]
                tweet = load_json(row[1])

                if not tweet:
                    continue

                min_date = datetime(2016, 9, 1)
                max_date = datetime(2017, 9, 1)

                created_at = parse(tweet['created_at'], ignoretz=True)
                if (created_at >= min_date) & (created_at < max_date):
                    retweet_id = None
                    retweet_truncated = False
                    truncated = False
                    status = None
                    refetched = False
                    error = None

                    if tweet['truncated']:
                        truncated = True
                        try:
                            status = api.get_status(tweet_id, tweet_mode='extended')
                            refetched = True
                        except tweepy.TweepError as e:
                            error = str(e)

                    if 'retweeted_status' in tweet:
                        retweet_id = tweet['retweeted_status']['id_str']
                        if tweet['retweeted_status']['truncated']:
                            retweet_truncated = True

                    row = [tweet_id, created_at, json.dumps(tweet), truncated, refetched, error, retweet_id, retweet_truncated]
                    writer.writerow(row)
