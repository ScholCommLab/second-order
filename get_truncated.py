import tweepy
import pandas as pd
import json
import requests

from urllib.parse import urlparse

from tqdm import tqdm

import os.path

import gc 

import configparser
Config = configparser.ConfigParser()
Config.read('config.cnf')

consumer_key = Config.get('twittersfupubresearch', 'consumer_key')
consumer_secret = Config.get('twittersfupubresearch', 'consumer_secret')
access_token = Config.get('twittersfupubresearch', 'access_token')
access_token_secret = Config.get('twittersfupubresearch', 'access_token_secret')

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# set up access to the Twitter API
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

with open('queries/queryInfo.txt', 'r') as queries:
    queries.readline()
    for f in queries: 
        query = f.split('\t')[0]
        relevant = query.split(' ')[0]
        if 'boston' in relevant:
            break

tweets = pd.read_csv('queries/%s.txt' % query, sep='\t', header=None)
tweets.columns = ['tweet_id', 'tweet']


def load_json(x):
    try:
        return json.loads(x, strict=False)
    except:
        print("Problematic tweet found.")
        return None


tweets['tweet'] = tweets.tweet.map(load_json)
tweets.dropna(subset=['tweet'], inplace=True)
tweets['created_at'] = tweets.tweet.map(lambda t: t['created_at'])
tweets['created_at'] = pd.to_datetime(tweets.created_at)
tweets = tweets[tweets['created_at'] < '2017-09-27'] # URL when expanded tweets were introduced

truncated = set(tweets[tweets.tweet.map(lambda x: x['truncated'])].tweet_id)
truncated = list(truncated.union(set(tweets[tweets.tweet.map(lambda x: x['retweeted_status']['truncated'] if 'retweeted_status' in x else False)].tweet_id)))

filename = 'queries/%s_extended.txt' % query
if os.path.exists(filename):
    writemode = 'w'
else:
    writemode = 'a'
    
with open(filename, writemode) as outfile: 
    for t in truncated:
        try: 
            status = api.get_status(t, tweet_mode='extended')
            outfile.write("%s\t%s\n" % (status.id, json.dumps(status._json)))
        except tweepy.TweepError as error: 
            pass