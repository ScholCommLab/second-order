#!/usr/bin/env python
# -*- coding: utf-8 -*-
 
import pandas as pd
from os import listdir
from os.path import isfile, join
import json
from pathlib import Path
from pprint import pprint
import urltools
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from tqdm import tqdm
tqdm.pandas()

data_path = Path("../../data/")

path_queries = data_path / "queries"
path_relevant = data_path / "relevant"
path_refetched = data_path / "refetched"
path_final = data_path / "final"

files = [f for f in listdir(str(path_relevant)) if isfile(join(str(path_relevant), f))]

queries = {
    'bostonglobe': ['science', 'science_extended'],
    'chicago': ['science'],
    'foxnews': ['science'],
    'theguardian': ['science'],
    'iflscience': [],
    'latimes': ['science'],
    'nytimes': ['science'],
    'sfchronicle': ['science'],
    'slate': ['bad_astronomy', 'climate_desk', 'future_tense', 'health_and_science'],
    'theglobeandmail': ['science'],
    'washingtonpost': ['animalia', 'energy-environment', 'speaking-of-science', 'to-your-health'],
    'wired': ['science']
}

news_short_altmetric = {
    'bostonglobe': ['The Boston Globe'],
    'chicago': ['Chicago Sun-Times'],
    'foxnews': ['FOX News'],
    'theguardian': ['The Guardian'],
    'iflscience': [],
    'latimes': ['LA Times'],
    'nytimes': ['New York Times'],
    'sfchronicle': ['San Francisco Chronicle'],
    'slate': ['Slate Magazine', 'Slate France'],
    'theglobeandmail': ['The Globe and Mail'],
    'washingtonpost': ['Washington Post'],
    'wired': ['Wired.it', 'Wired.com', 'Wired.co.uk']
}

news_altmetric_short = {}
for a, ss in news_short_altmetric.items():
    for s in ss:
        news_altmetric_short[s] = a


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


def get_domain(url):
    '''
    Strip out trailing slashes, URL query variables, anchors, etc.
    '''
    if url:
        try:
            up = urlparse(url)
            domain = '.'.join(up.netloc.split('.')[-2:]).strip()
            return domain
        except:
            raise
    else:
        return "None"


def load_urls(file):
    urls = pd.read_csv(file,
                       na_values="None",
                       dtype={'tweet_id': str,
                              'relevant_url': str,
                              'cleaned_url': str})
    urls = urls.drop_duplicates()
    urls = urls.set_index("tweet_id")
    return urls


def load_tweets(file):
    tweets = pd.read_csv(file,
                         dtype={'tweet_id': str,
                                'retweet_id': str},
                         parse_dates=['posted_on'])
    tweets = tweets.drop_duplicates(subset=["tweet_id", "posted_on"])
    tweets = tweets.set_index("tweet_id")
#     tweets['retweet_id'] = tweets.retweet_id.map(lambda x: np.nan if pd.isna(x) else int(x))
    return tweets


def clean_url(url, v):
    '''
    Strip out trailing slashes, URL query variables, anchors, etc.
    '''
    if pd.isna(url):
        return np.nan

    url = "www." + v + "".join(url.split(v)[1:])

    try:
        up = urltools.extract(url)
        url = up.domain + "." + up.tld + up.path
        url = urltools.normalize(url)
        return url
    except:
        raise


def merge_urls(row):
    a = row['relevant_url']
    b = row['retweet_url']
    if not pd.isna(a):
        return a

    return b


def relevant_url(url, v):
    '''
    Strip out trailing slashes, URL query variables, anchors, etc.
    '''
    if pd.isna(url):
        return False

    for category in queries[v]:
        if "/" + category + "/" in url:
            return True
    return False

for file in tqdm(files):
    if isfile(str(path_final / file)):
        print("{} already exists".format(file))
        continue

    v = file.split(" ")[0]

    urls = load_urls(path_relevant / file)
    tweets = load_tweets(path_refetched / file)

    urls = urls.merge(tweets[['retweet_id']], left_index=True,
                      right_index=True, how="left", validate="one_to_one")
    urls['retweet_url'] = None

    rtwts_with_urls = urls[(urls['retweet_id'].notnull()) & (
        urls['relevant_url'].notnull())].retweet_id
    rtwts_wo_urls = urls[(urls['retweet_id'].notnull()) & (
        urls['relevant_url'].isnull())].retweet_id

    # Copy relevant urls for retweeted statuses from original ones
    for ix, row in urls[urls.retweet_id.isin(rtwts_wo_urls)].iterrows():
        rt_id = row['retweet_id']
        if rt_id in urls.index:
            if urls.loc[rt_id, 'relevant_url']:
                urls.loc[ix, 'retweet_url'] = urls.loc[rt_id, 'relevant_url']

    urls['url'] = urls.apply(merge_urls, axis=1)

    urls['cleaned_url'] = urls.url.map(lambda x: clean_url(x, v))
    urls['relevant'] = urls.cleaned_url.map(lambda x: relevant_url(x, v))

    urls.to_csv(path_final / file, header=True)
