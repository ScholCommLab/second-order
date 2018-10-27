import configparser
import csv
import json
import operator
import os.path
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from dateutil.parser import parse
from ratelimit import limits, sleep_and_retry

try:  # for notebooks
    get_ipython
    from tqdm._tqdm_notebook import tqdm_notebook as tqdm
except:  # for commandline
    from tqdm import tqdm
tqdm.pandas()


def load_json(x):
    try:
        return json.loads(x, strict=False)
    except:
        print("Problematic tweet found.")
        return None


def get_tweet_urls(t):
    '''
    Given a Tweet JSON, pull the URLs found inside it
    '''
    try:
        return get_urls(t['entities']['urls'])
    except:
        return []


def get_retweet_urls(t):
    '''
    Given a Tweet JSON, pull the URLs of the Tweet this tweet retweeted
    '''
    try:
        return get_urls(t['retweeted_status']['entities']['urls'])
    except:
        return []


def get_urls(urls):
    '''
    Generic function to extract the URLs from the urls sub-object
    '''
    try:
        urls = [v for (k, v) in urls[0].items()
                if k in ('url', 'expanded_url')]
        return list(set(urls))
    except:
        return []


def match_urls(urls, to_match):
    '''
    urls: all the URLs found in the tweet or retweet
    to_match: a substring to figure out what URLs are actually relevant
    '''
    return [url for url in urls if to_match in url]


def clean_url(url):
    '''
    Strip out trailing slashes, URL query variables, anchors, etc.
    '''
    if url:
        try:
            up = urlparse(url)
            domain = '.'.join(up.netloc.split('.')[-2:]).strip()
            path = up.path.strip('/').strip()
            return '{}/{}'.format(domain, path)
        except:
            raise
    else:
        return "None"


class TooManyPublisherRequests(Exception):
    pass


class PublisherTracker(object):
    def __init__(self):
        self.count = 0
        self.max = 300
        self.publishers = ['elsevier', 'springer', 'amc']

    def check_url(self, url):
        for pub in self.publishers:
            if pub in url.lower():
                self.count += 1
                if self.count > self.max:
                    raise TooManyPublisherRequests()


@sleep_and_retry
@limits(calls=30, period=1)
def resolve_url(url, session, timeout=5):
    try:
        resp = session.get(url, allow_redirects=True, timeout=timeout)
        return resp.url, None
    except Exception as e:
        return None, e


def expand_urls(tweet_id, urls, session, expanded_urls_df, exp_writer, relevant_string, pub_tracker):
    '''
    Expand all URLs.
    '''
    try:
        for url in urls:
            if url in expanded_urls_df.keys():
                if expanded_urls_df[url]['error'] == "None":
                    resolved_url = expanded_urls_df[url]['resolved_url']
                    error = None
                    return resolved_url
                else:
                    resolved_url, error = resolve_url(url, session)
            else:
                resolved_url, error = resolve_url(url, session)

            now = str(datetime.now())

            expanded_urls_df[url] = {
                'tweet_id': tweet_id,
                'venue': relevant_string,
                'resolved_url': resolved_url,
                'error': error,
                'timestamp': now
            }

            exp_writer.writerow([url, tweet_id, relevant_string, resolved_url, error, now])

            if resolved_url:
                pub_tracker.check_url(url)
                if match_urls([resolved_url], relevant_string):
                    return resolved_url
    except TooManyPublisherRequests:
        raise


if __name__ == "__main__":
    data_dir = Path("../data/")
    input_dir = data_dir / "refetched/"
    output_dir = data_dir / "relevant/"

    # Read in files and their row sizes
    row_counts = {}
    with open(str(input_dir / 'row_counts.txt')) as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            row_counts[row[0]] = int(row[1])

    pub_tracker = PublisherTracker()

    # keep track of urls that were already expanded
    col_names = ['tweet_id', 'venue', 'resolved_url', 'error', 'timestamp']
    expanded_urls_df = {}

    with open(str(output_dir / "expanded_urls.csv"), 'w') as f:
        exp_writer = csv.writer(f)
        exp_writer.writerow(['short_url'] + col_names)

        # Sort files from small to big
        sorted_files = sorted(row_counts.items(), key=operator.itemgetter(1))
        for filename, rows in sorted_files:
            query_file = input_dir / filename
            if not query_file.exists():
                continue

            # CSV for the final map
            outfile = output_dir / filename
            outfile_headers = ['tweet_id', 'relevant_url', 'cleaned_url', 'expanded']

            # If file already exists, headers are not written
            skip_header = False
            if outfile.exists():
                skip_header = True

            # Create a requests session to be re-used throughout the script
            session = requests.Session()

            # Search string and filename
            relevant_string = filename.split(" ")[0]
            if relevant_string == "chicago":
                relevant_string = "suntimes"

            print("Processing {}".format(filename))
            with open(str(query_file), "r") as infile:
                with open(str(outfile), "w") as outfile:
                    reader = csv.reader(infile)
                    next(reader, None)  # skip headers
                
                    writer = csv.writer(outfile)
                    if not skip_header:
                        writer.writerow(outfile_headers)
                    
                    for row in tqdm(reader, total=rows-1):
                        tweet_id = row[0]
                        tweet = load_json(row[2])

                        # Skip if there's no tweet
                        if not tweet:
                            continue

                        try:
                            # Get tweet & retweet URLs
                            tweet_urls = get_tweet_urls(tweet)
                            retweet_urls = get_retweet_urls(tweet)
                            urls = set(tweet_urls + retweet_urls)

                            # Match with the selected news venue
                            relevant_urls = match_urls(urls, relevant_string)
                            
                            if len(relevant_urls) > 0:
                                relevant_url = relevant_urls[0]
                                expanded = False
                            else:
                                # remove URL that contain 'twitter.com'
                                urls = [
                                    url for url in urls if 'twitter.com' not in url]

                                relevant_url = expand_urls(tweet_id, urls, session, expanded_urls_df, exp_writer, relevant_string, pub_tracker)
                                expanded = True

                            writer.writerow([str(tweet_id), str(relevant_url), clean_url(relevant_url), expanded])

                        except TooManyPublisherRequests:
                            print("Too many requests to publishers")
                            sys.exit(0)
