#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
1. Read original tweet JSONs.
2. Filter time window.
3. Re-fetch truncated tweets.

Input: ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
        'quoted_status', 'in_reply_to', 'urls', 'is_truncated']

Output: ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
          'quoted_status', 'in_reply_to', 'urls', 'is_truncated',
          'refetched', 'error']
"""

import configparser
import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import tweepy
from dateutil.parser import parse
from tqdm import tqdm

from helpers import *

tqdm.pandas()


def refetch_tweet(tweet_id):
    error = None

    try:
        status = api.get_status(tweet_id, tweet_mode='extended')
        tweet = status._json
    except tweepy.TweepError as e:
        error = str(e)
        return None, True, False, error

    if tweet_id != tweet['id_str']:
        return None, True, False, "Fetched wrong tweet: {}".format(tweet['id_str'])

    urls = get_tweet_urls(tweet)
    if len(urls) > 0:
        urls = str(urls)
    else:
        urls = None

    return urls, False, True, error


if __name__ == "__main__":
    min_date = datetime(2016, 9, 1)
    max_date = datetime(2017, 9, 1)

    headers = ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
               'quoted_status', 'in_reply_to', 'urls', 'is_truncated',
               'refetched', 'error']

    # Load config
    Config = configparser.ConfigParser()
    Config.read('../../config.cnf')

    consumer_key = Config.get('twitter_keys', 'consumer_key')
    consumer_secret = Config.get('twitter_keys', 'consumer_secret')
    access_token = Config.get('twitter_keys', 'access_token')
    access_token_secret = Config.get('twitter_keys', 'access_token_secret')

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    # set up access to the Twitter API
    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    # Load files from disk
    queries = load_queries("../../data/queries.csv")

    data_dir = Path("../../data/")
    input_dir = data_dir / "tweets/"
    output_dir = data_dir / "refetched_tweets/"

    files = input_dir.glob("*.csv")

    # Iterate over available newspapers
    for infile in files:
        outfile = output_dir / infile.name
        if outfile.exists():
            print("{} already exists. skipping".format(outfile))
            continue

        q = infile.name.split(".")[0]
        row_count = queries.loc[queries['query'] == q, "found_tweets"].iloc[0]

        print("Collecting {}".format(infile.name))
        with open(str(infile), "r") as inf:
            with open(str(outfile), "a") as outf:
                reader = csv.reader(inf)
                next(reader, None)

                writer = csv.writer(outf)
                writer.writerow(headers)

                for row in tqdm(reader, total=row_count):
                    posted_on = parse(row[headers.index('posted_on')], ignoretz=True)

                    if (posted_on >= min_date) & (posted_on < max_date):
                        if row[headers.index('is_truncated')]:
                            urls, truncated, refetched, error = refetch_tweet(row[0])
                            row[headers.index('urls')] = urls
                            row[headers.index('is_truncated')] = truncated
                            row.append(refetched)
                            row.append(error)
                        else:
                            row.append(None)
                            row.append(None)

                        writer.writerow(row)
