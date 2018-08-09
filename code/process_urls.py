import configparser
import csv
import json
import os.path
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests
from dateutil.parser import parse
from ratelimit import limits, sleep_and_retry
import operator

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
    try:
        up = urlparse(url)
        domain = '.'.join(up.netloc.split('.')[-2:]).strip()
        path = up.path.strip('/').strip()
        query = up.query
        if query:
            return '{}/{}?{}'.format(domain, path, query)
        else:
            return '{}/{}'.format(domain, path)
    except:
        raise


@sleep_and_retry
@limits(calls=10, period=1)
def expand_urls(urls, session, timeout=5):
    '''
    Expand all URLs. This function can only be called as many times per period
    as defined in the @limits decorator.
    '''
    resolved_urls = []
    for url in urls:
        try:
            resp = session.get(url, allow_redirects=True, timeout=timeout)
            resolved_urls.append(resp.url)
        except Exception as e:
            print(e)
    return resolved_urls


class PublisherException(Exception):
    '''
    User Exception Class to handle requests to big publishers
    '''
    pass


def too_many_publ_requests(urls):
    global publisher_count
    for pub in ['elsevier', 'springer', 'amc']:
        for url in urls:
            if pub in url.lower():
                publisher_count += 1
    return publisher_count > 100


def get_expanded_urls(tweet):
    '''
    Get tweeted and retweeted URLs and try to identify relevant ones.
    Also expands shortened URls
    '''
    # Get tweet & retweet URLs
    tweet_urls = get_tweet_urls(tweet)
    retweet_urls = get_retweet_urls(tweet)

    # Match with news venue
    relevant_urls = match_urls(tweet_urls + retweet_urls, relevant_string)

    if len(relevant_urls) == 0:
        shortened_urls = set(tweet_urls + retweet_urls)
        shortened_urls = [
            url for url in shortened_urls if 'twitter.com' not in url]

        relevant_urls = expand_urls(tweet_urls+retweet_urls, session)

        # Keep track of requests to Publishers and eventually stop execution
        if too_many_publ_requests(relevant_urls):
            raise PublisherException("Too many publisher URLs")

    return relevant_urls


def select_cleaned_url(relevant_urls):
    '''
    Clean all URLs and select one of the expanded ones
    '''
    # clean all urls
    relevant_urls = list(set([clean_url(url) for url in relevant_urls]))

    # check if news_name in url
    relevant_urls = [url for url in relevant_urls if relevant_string in url]

    return relevant_urls[0] if len(relevant_urls) > 0 else ''


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

    # Sort files from small to big
    sorted_files = sorted(row_counts.items(), key=operator.itemgetter(1))

    # global var to keep track of publisher requests
    publisher_count = 0

    for filename, rows in sorted_files:
        if 'chicago' not in filename:
            continue

        # Try to load current input file
        query_file = input_dir / filename
        if not query_file.exists():
            continue

        # CSV for the final map
        outfile = output_dir / filename
        outfile_headers = ['tweet_id', 'expanded_urls', 'relevant_url']

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

        """
        Loop over all tweets:
            1. Check if urls or expanded_url contain a relevant link
                a. If relevant, clean and write to output
                b. If not relevat:
                    i. Expand shortened url (keep track of elsevier, springer, acm)
                    ii. Check if relevant
        """
        print("Processing {}".format(filename))
        with open(str(query_file), "r") as infile:
            with open(str(outfile), "a") as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile)

                if not skip_header:
                    writer.writerow(outfile_headers)

                # CSV columns: tweet_id, posted_on, tweet, truncated,
                #   refetched, error, retweet_id, retweet_truncated
                next(reader, None)  # skip headers

                for row in tqdm(reader, total=rows):
                    tweet_id = row[0]
                    tweet = load_json(row[2])

                    if not tweet:
                        continue

                    try:
                        expanded_urls = get_expanded_urls(tweet)
                        relevant_url = select_cleaned_url(expanded_urls)

                        writer.writerow([str(tweet_id), str(relevant_urls), relevant_url])
                    except PublisherException:
                        print("Too many requests to publishers")
                        sys.exit(0)
