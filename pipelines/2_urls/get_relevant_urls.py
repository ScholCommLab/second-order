#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
1. Iterate over tweets and match URLs with terms
2. If none, expand URLs until URL matches with term

Output file:
    - tweet_id
    - relevant_url
    - clean_url
    - expanded
"""

import configparser
import csv
import json
import logging
import operator
import os.path
import sys
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from dateutil.parser import parse
from ratelimit import limits, sleep_and_retry
from tqdm import tqdm

from _helpers import *

tqdm.pandas()


class TooManyPublisherRequests(Exception):
    pass


class PublisherTracker(object):
    def __init__(self):
        self.max = 300
        self.publishers = ['elsevier', 'springer', 'amc']
        self.count = {pub: 0 for pub in self.publishers}

    def check_url(self, url):
        for pub in self.publishers:
            if pub in url.lower():
                self.count[pub] += 1
                if sum(self.count.values()) > self.max:
                    raise TooManyPublisherRequests()

    def save_csv(self, file):
        df = pd.DataFrame.from_dict(self.count, orient="index", columns=["requests"])
        df.index.name = "publisher"
        df.to_csv(file, index=True)


@sleep_and_retry
@limits(calls=30, period=1)
def resolve_url(url, session, timeout=5):
    try:
        resp = session.get(url, allow_redirects=True, timeout=timeout)
        return resp.url, None
    except Exception as e:
        return None, e


# file headers
infile_headers = ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
                  'quoted_status', 'in_reply_to', 'urls', 'is_truncated',
                  'refetched', 'error']
outfile_headers = ['tweet_id', 'relevant_url', 'expanded', 'timestamp']
exp_url_headers = ['short_url', 'resolved_url', 'error', 'timestamp']

# Local files
output_dir = Path("temp/")
exp_file = str(output_dir / "expanded_urls.csv")
pub_tracker_file = str(output_dir / "publisher_requests.csv")
log_file = str(output_dir / "log.txt")

# Setup logger
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler(log_file)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

if __name__ == "__main__":
    # Load config
    logger.info('Loading configuration.')
    root = Path('../../')
    Config = configparser.ConfigParser()
    Config.read(str(root / 'config.cnf'))

    # Load files from disk
    queries = str(root / Config.get('input_files', 'queries'))
    queries = load_queries(str(queries))

    input_files = root / Config.get('output_files', 'tweets')

    # Expanded URLs
    logger.info("Trying to load previously expanded URLs and prepare file stream")
    try:
        exp = pd.read_csv("expanded_urls.csv", index_col="id")
    except:
        exp = pd.DataFrame(columns=exp_url_headers)
        exp.index.name = "id"
        exp.to_csv(exp_file, index=True)

    # Init publication tracker
    pub_tracker = PublisherTracker()

    files = input_files.glob("*.csv")
    for infile in files:
        # Skip files that start with _ (IFLscience, Chicago-Suntimes)
        if infile.name[0] == "_":
            logger.info("Skipping {}".format(infile.name))
            continue

        # Final output file
        outfile = output_dir / infile.name
        if outfile.exists():
            appending_mode = True
            previous_tweets = pd.read_csv(outfile)
            logger.info("URLs for {} already exist. Appending new rows.".format(infile.name))
        else:
            appending_mode = False
            previous_tweets = pd.DataFrame(columns=outfile_headers)
            logger.info("URLs for {} do not exist. Creating new file..".format(infile.name))

        session = requests.Session()

        logger.info("Processing tweets in {}".format(infile.name))

        # Open file stream
        infile = open(str(infile), "r")
        outfile = open(str(outfile), "a")

        reader = csv.reader(infile)
        next(reader, None)  # skip headers
        count = sum(1 for _ in reader)  # count number of rows
        infile.seek(0)

        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        if not appending_mode:
            writer.writerow(outfile_headers)

        # Load relevant terms
        query = infile.name.split("/")[-1].split(".")[0]
        venue_short = query.split(" ")[0]

        terms = queries.groupby("venue_short")['relevant_terms'].apply(lambda x: list(x))
        terms = terms[venue_short]

        for row in tqdm(reader, total=count):
            tweet_id = row[0]

            if tweet_id in previous_tweets.tweet_id.tolist():
                continue

            tweet = load_json(row[2])
            if not tweet:
                logging.debug("Couldn't load tweet")
                continue

            try:
                # Get tweet & retweet URLs
                s = row[infile_headers.index('urls')]
                if pd.isna(s) or s is "":
                    continue
                url_candidates = json.loads(s)

                # remove links to twitter urls
                url_candidates = [url for url in url_candidates if 'twitter.com' not in url]

                found_url = None
                # Match with the selected news venue

                for url in url_candidates:
                    if relevant_url(url, venue_short, terms):
                        found_url = url
                        expanded = False
                        break

                if not found_url:
                    for url in url_candidates:
                        if url in exp.short_url.tolist():
                            df = exp[exp.short_url == url]
                            df = df[df['error'].isnull()]
                            if len(df) == 0:
                                logger.debug("Resolving URL that previously failed.")
                                r_url, error = resolve_url(url, session)
                                exp.loc[len(exp)+1] = [url, r_url, error, str(datetime.now())]
                                with open(exp_file, 'a') as f:
                                    writer = csv.writer(f)
                                    writer.writerow(
                                        [len(exp), url, r_url, error, str(datetime.now())])
                                url = r_url
                            else:
                                url = df.iloc[0]["resolved_url"]
                        else:
                            logger.debug("New URL. Resolving.")
                            r_url, error = resolve_url(url, session)
                            exp.loc[len(exp)+1] = [url, r_url, error, str(datetime.now())]
                            with open(exp_file, 'a') as f:
                                writer = csv.writer(f)
                                writer.writerow(
                                        [len(exp), url, r_url, error, str(datetime.now())])
                            url = r_url

                        if relevant_url(url, venue_short, terms):
                            found_url = url
                            expanded = True
                            break

                if found_url:
                    pub_tracker.check_url(found_url)

                    now = str(datetime.now())
                    writer.writerow([str(tweet_id), str(found_url), expanded, now])

            except TooManyPublisherRequests:
                logger.exception("Too many requests to publishers")
                pub_tracker.save_csv(pub_tracker_file)
                sys.exit(0)

        # Write expanded URLs
        infile.close()
        outfile.close()
    f.close()
    pub_tracker.save_csv(pub_tracker_file)
