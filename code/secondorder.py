
# coding: utf-8

import json
from sys import argv
from urllib.parse import urlparse

import pandas as pd
import requests

try:  # for notebooks
    get_ipython
    from tqdm._tqdm_notebook import tqdm_notebook as tqdm
except:  # for commandline
    from tqdm import tqdm
tqdm.pandas()


script, filename = argv


tweets = pd.read_csv('queries/%s' % filename, sep='\t', header=None)
tweets.columns = ['tweet_id', 'tweet']
tweets['tweet'] = tweets.tweet.map(json.loads)


def get_tweet_urls(t):
    try:
        return get_urls(t['entities']['urls'])
    except:
        return []


def get_retweet_urls(t):
    try:
        return get_urls(t['retweeted_status']['entities']['urls'])
    except:
        return []


def get_urls(urls):
    try:
        urls = [v for (k, v) in urls[0].items()
                if k in ('url', 'expanded_url')]
        return list(set(urls))
    except:
        return []


tweets['tweet_urls'] = tweets.tweet.map(get_tweet_urls)
tweets['retweet_urls'] = tweets.tweet.map(get_retweet_urls)


def match_urls(urls, to_match):
    return [url for url in urls if to_match in url]


tweets['relevant_urls'] = tweets.apply(lambda row: match_urls(
    row['tweet_urls']+row['retweet_urls'], 'globe'), axis=1)


def clean_url(url):
    try:
        up = urlparse(url)
        domain = '.'.join(up.netloc.split('.')[-2:]).strip()
        path = up.path.strip('/').strip()
        return '%s/%s' % (domain, path)
    except:
        raise


tweets['relevant_urls'] = tweets.relevant_urls.map(
    lambda urls: list(set([clean_url(url) for url in urls])))
tweets['clean_url'] = tweets.relevant_urls.map(
    lambda x: x[0] if len(x) > 0 else '')


no_relevant = tweets[tweets.relevant_urls.map(len) == 0]
shortened_urls = set(no_relevant.tweet_urls.sum() +
                     no_relevant.retweet_urls.sum())
shortened_urls = [url for url in shortened_urls if 'twitter.com' not in url]

print(len(shortened_urls))


with open('shortened_urls.txt', 'w') as f:
    f.write('short_url\n')
    for url in shortened_urls:
        f.write('%s\n' % url)


resolved_urls = pd.read_csv('resolved_urls.csv')
resolved_urls.dropna(subset=['url'], inplace=True)
resolved_urls = resolved_urls.set_index('short_url').to_dict()['url']


def expand_urls(urls):
    global resolved_urls
    return [resolved_urls[url] for url in urls if url in resolved_urls]


tweets['expanded_tweet_urls'] = tweets.apply(lambda row: expand_urls(
    row['tweet_urls']) if len(row['relevant_urls']) == 0 else [], axis=1)
tweets['expanded_retweet_urls'] = tweets.apply(lambda row: expand_urls(
    row['retweet_urls']) if len(row['relevant_urls']) == 0 else [], axis=1)


tweets['expanded_relevant_urls'] = tweets.apply(lambda row: match_urls(
    row['expanded_tweet_urls']+row['expanded_retweet_urls'], 'globe'), axis=1)


tweets['expanded_relevant_urls'] = tweets.expanded_relevant_urls.map(
    lambda urls: list(set([clean_url(url) for url in urls])))
tweets['expanded_clean_url'] = tweets.expanded_relevant_urls.map(
    lambda x: x[0] if len(x) > 0 else '')


tweets.clean_url.map(lambda x: len(x) > 0).sum()


tweets['clean_url'] = tweets.apply(lambda row: row['clean_url'] if len(
    row['clean_url']) > 0 else row['expanded_clean_url'], axis=1)


# def get_linked_tweet(urls):
#     twitter_urls = match_urls(urls, 'twitter.com')
#     try:
#         tweet_id = twitter_urls[0].split('/')[-1]
#         if tweet_id.isnumeric():
#             return int(tweet_id)
#     except:
#         pass
#     return 0

# tweets['link_to_tweet'] = tweets.apply(lambda row: get_linked_tweet(row['tweet_urls'] + row['retweet_urls']), axis=1)

# # sometimes the tweet link is to itself. Set to 0
# tweets['link_to_tweet'] = tweets.apply(lambda row: row['link_to_tweet'] if row['link_to_tweet'] != row['tweet_id'] else 0, axis=1)


# Try ths again after finding more relevant URLs. This is currently not yielding anything.

# df = tweets[tweets.clean_url.map(len) > 0][['tweet_id', 'clean_url']]
# df.columns = ['tweet_id', 'link_to_tweet_clean_url']
# df.sample(10)
# df2 = tweets.merge(df, left_on='link_to_tweet', right_on='tweet_id', how='left')


# # mentions = pd.read_excel('theglobeandmail.xlsx')
# mentions = pd.read_excel('News_mentions_2017.xlsx')
# mentions = mentions[['Altmetric_ID', 'Url']]
# mentions['clean_url'] = mentions.Url.map(clean_url)
# mentions.sample(10)
# df = tweets.merge(mentions, left_on='clean_url', right_on='clean_url', how='left')
# len(df.Altmetric_ID.unique())
