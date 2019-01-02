#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

import numpy as np
import pandas as pd
import urltools


def load_tweets(x):
    """
    Load files containing tweets
    """
    return pd.read_csv(x,
                       dtype={
                           'tweet_id': str,
                           'user_id': str,
                           'retweeted_status': str,
                           'quoted_status': str,
                           'in_reply_to': str
                       })


# Loading files
def load_urls(file):
    '''
    Load file that contains tweets & urls
    '''
    urls = pd.read_csv(file,
                       na_values="None",
                       dtype={'tweet_id': str,
                              'retweeted_status': str,
                              'quoted_status': str,
                              'relevant_url': str,
                              'cleaned_url': str},
                       parse_dates=['timestamp'])
    # urls = urls.drop_duplicates()
    # urls = urls.set_index("tweet_id")
    return urls


def load_queries(file):
    '''
    Load file that contains information about the search queries
    '''
    queries = pd.read_csv(file, index_col="id")
    return queries


def load_json(x):
    '''
    Load Tweet JSON
    '''
    try:
        return json.loads(x, strict=False)
    except:
        return None


def clean_url(url, venue=None):
    '''
    Strip out trailing slashes, URL query variables, anchors, etc.
    '''
    if pd.isna(url):
        return np.nan

    # remove parts of the URL that come before the domain (e.g., google news)
    if venue:
        url = "www." + venue + "".join(url.split(venue)[1:])

    try:
        up = urltools.extract(url)
        url = up.subdomain + "." + up.domain + "." + up.tld + up.path
        url = urltools.normalize(url)
        return url
    except:
        raise


def relevant_url(url, venue, terms):
    '''
    Check if URL contains one of the search terms for each news venue
    in the form "/search_term/" (which would typically be a section)
    '''
    if pd.isna(url):
        return False

    if venue not in url:
        return False

    for term in terms:
        if "/" + term + "/" in url:
            return True
    return False
