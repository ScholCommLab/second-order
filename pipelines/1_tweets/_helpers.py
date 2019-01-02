#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

import numpy as np
import pandas as pd
import urltools


# Loading individual tweets and manipulate them
def load_json(x):
    '''
    Load Tweet JSON
    '''
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
                              'retweet_id': str,
                              'relevant_url': str,
                              'clean_url': str})
    urls = urls.drop_duplicates()
    urls = urls.set_index("tweet_id")
    return urls


def load_queries(file):
    '''
    Load file that contains information about the search queries
    '''
    queries = pd.read_csv(file, index_col="id")
    return queries


# URL operations
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
        url = up.domain + "." + up.tld + up.path
        url = urltools.normalize(url)
        return url
    except:
        raise


def relevant_url(url, terms):
    '''
    Check if URL contains one of the search terms for each news venue
    in the form "/search_term/" (which would typically be a section)
    '''
    if pd.isna(url):
        return False

    for term in terms:
        if "/" + term + "/" in url:
            return True
    return False


def merge_urls(row):
    '''
    If relevant URL exists return it, otherwise
    use retweeted URL
    '''
    if not pd.isna(row['relevant_url']):
        return row['relevant_url']
    return row['retweet_url']
