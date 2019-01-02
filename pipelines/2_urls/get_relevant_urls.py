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

import sys
sys.path.append("../")

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
@limits(calls=1, period=1)
def resolve_url(url, session, timeout=5):
    try:
        resp = session.get(url, allow_redirects=True, timeout=timeout)
        return resp.url, None
    except Exception as e:
        return None, e


def exp_write_row(f, row):
    with open(str(f), 'a') as f:
        exp_writer = csv.writer(f)
        exp_writer.writerow(row)


# file headers
infile_headers = ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
                  'quoted_status', 'in_reply_to', 'urls', 'is_truncated',
                  'refetched', 'error']
outfile_headers = ['tweet_id', 'relevant_url', 'expanded', 'timestamp']
exp_url_headers = ['short_url', 'resolved_url', 'error', 'timestamp']

# Local files
output_dir = Path("temp/")
exp_file = output_dir / "expanded_urls.csv"
pub_tracker_file = output_dir / "publisher_requests.csv"
log_file = output_dir / "log.txt"

# Setup logger
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh = logging.FileHandler(str(log_file))
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

if __name__ == "__main__":
    # Load config
    logger.info('# Loading configuration.')
    root = Path('../../')
    Config = configparser.ConfigParser()
    Config.read(str(root / 'config.cnf'))

    # Load files from disk
    queries = root / Config.get('input_files', 'queries')
    queries = load_queries(str(queries))

    temp_tweets = root / "pipelines/2_urls/temp/"
    input_files = list(temp_tweets.glob("*.csv"))

    # Expanded URLs
    logger.info("# Trying to load previously expanded URLs")
    if exp_file.exists():
        exp = pd.read_csv(str(exp_file), index_col=0)
    else:
        exp = pd.DataFrame(columns=exp_url_headers)
        exp.to_csv(str(exp_file), index=True)

    # # Clean and update exp_urls on disk
    # exp = exp.drop_duplicates(subset=['short_url', 'resolved_url'])
    # exp.reset_index(drop=True)
    # exp.to_csv(str(exp_file), index=True)

    expanded_url_count = len(exp)

    # Create dicts and sets for looping
    good_exp_urls = {}
    for row in exp[exp.error.isnull()][['short_url', 'resolved_url']].itertuples():
        good_exp_urls[row[1]] = row[2]  # if multiple urls, we just take the latest resolved one
    bad_exp_urls = set(exp[exp.error.notnull()].short_url.tolist())

    # Init publication tracker
    pub_tracker = PublisherTracker()

    for infile in input_files:
        logger.info("## Processing {} ".format(infile.name))
        # Skip files that start with _ (IFLscience, Chicago-Suntimes)
        if infile.name[0] == "_":
            logger.info("## Skipping {}")
            continue

        # Final output file
        outfile = output_dir / infile.name
        if outfile.exists():
            logger.info("## URLs already exist. Appending new rows.")
            previous_tweets = pd.read_csv(str(outfile), dtype={'tweet_id': str})
        else:
            logger.info("## URLs do not exist. Creating new file..")
            previous_tweets = pd.DataFrame(columns=outfile_headers)
            previous_tweets.to_csv(str(outfile), index=False)

        previous_tweet_ids = set(previous_tweets.tweet_id.tolist())
        logger.info("## Found {} relevant URLs".format(len(previous_tweet_ids)))

        # Open file stream
        logger.info("## Creating file streams")
        infile = open(str(infile), "r")
        reader = csv.reader(infile)
        count = sum(1 for _ in reader) - 1  # count number of rows minus header
        infile.seek(0)
        reader = csv.reader(infile)
        next(reader, None)

        outfile = open(str(outfile), "a")
        writer = csv.writer(outfile)

        # Load relevant terms
        query = infile.name.split("/")[-1].split(".")[0]
        venue_short = query.split(" ")[0]

        terms = queries.groupby("venue_short")['relevant_terms'].apply(lambda x: list(x))
        terms = terms[venue_short]

        session = requests.Session()

        for row in tqdm(reader, total=count):
            tweet_id = row[0]

            if tweet_id in previous_tweet_ids:
                logger.debug("### {}: Tweet already has relevant URL.".format(tweet_id))
                continue

            tweet = load_json(row[2])
            if not tweet:
                logging.debug("### {}: Couldn't load tweet".format(tweet_id))
                continue

            try:
                # Get tweet & retweet URLs
                s = row[infile_headers.index('urls')]
                if pd.isna(s) or s is "":
                    logger.debug("### {}: Tweet contains no URLs.".format(tweet_id))
                    continue
                url_candidates = json.loads(s)

                # remove links to twitter urls
                url_candidates = [url for url in url_candidates if 'twitter.com' not in url]

                found_url = None
                expanded = False
                # Match with the selected news venue
                urls_to_remove = []
                for url in url_candidates:
                    # Check if URL in tweet is relevant
                    if relevant_url(url, venue_short, terms):
                        logger.debug("### {}: Found relevant link in tweet.".format(tweet_id))
                        found_url = url
                        break
                    # Check if previously resolved URL is relevant
                    if url in good_exp_urls:
                        r_url = good_exp_urls[url]
                        if relevant_url(r_url, venue_short, terms):
                            logger.debug("### {}: Reusing previous resolve.".format(tweet_id))
                            found_url = r_url
                            expanded = True
                            break
                        else:
                            urls_to_remove.append(url)

                url_candidates = [url for url in url_candidates if url not in urls_to_remove]

                if not found_url:
                    for url in url_candidates:
                        r_url, error = None, None
                        # Find candidate URL in existing data or with resolving
                        if url in bad_exp_urls:
                            logger.debug(
                                "### {}: Resolving URL that previously failed.".format(tweet_id))
                            r_url, error = resolve_url(url, session)
                            expanded = True

                            if r_url:
                                good_exp_urls[url] = r_url
                                bad_exp_urls.remove(url)

                                row = [expanded_url_count, url, r_url, error, str(datetime.now())]
                                exp_write_row(str(exp_file), row)
                                expanded_url_count = expanded_url_count + 1
                        else:
                            logger.debug("### {}: New URL. Resolving.".format(tweet_id))
                            r_url, error = resolve_url(url, session)
                            expanded = True

                            row = [expanded_url_count, url, r_url, error, str(datetime.now())]
                            exp_write_row(str(exp_file), row)
                            expanded_url_count = expanded_url_count + 1

                            if r_url:
                                good_exp_urls[url] = r_url
                            else:
                                bad_exp_urls.add(url)

                        if r_url:
                            if relevant_url(r_url, venue_short, terms):
                                found_url = r_url
                                break

                if found_url:
                    pub_tracker.check_url(found_url)

                    now = str(datetime.now())
                    writer.writerow([str(tweet_id), str(found_url), expanded, now])
                    outfile.flush()
            except TooManyPublisherRequests:
                logger.exception("Too many requests to publishers")
                pub_tracker.save_csv(pub_tracker_file)
                sys.exit(0)
        # Close file streams
        infile.close()
        outfile.close()
    pub_tracker.save_csv(pub_tracker_file)
