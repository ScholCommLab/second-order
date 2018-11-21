#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This script transforms results for the tweet_refetching
from the old format to the new one.

Only for experimental purposes.

Input: [tweet_id, posted_on, tweet, truncated, refetched,
        error, retweet_id, retweet_truncated]

Output: ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
          'quoted_status', 'in_reply_to', 'urls', 'is_truncated',
          'refetched', 'error']
'''

import csv
from pathlib import Path
from tqdm import tqdm
from dateutil.parser import parse

from _helpers import *

if __name__ == "__main__":
    input_cols = ['tweet_id', 'posted_on', 'tweet', 'truncated',
                  'refetched', 'error', 'retweet_id', 'retweet_truncated']

    output_cols = ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
                   'quoted_status', 'in_reply_to', 'urls', 'is_truncated',
                   'refetched', 'error']

    data_dir = Path("../../data/")
    input_dir = data_dir / "refetched_tweets_old/"
    output_dir = data_dir / "refetched_tweets/"

    files = input_dir.glob("*.csv")

    # Iterate over available newspapers
    for infile in files:
        outfile = output_dir / infile.name
        if outfile.exists():
            print("{} already exists. skipping".format(outfile))
            continue

        print("Collecting {}".format(infile.name))
        with open(str(infile), "r") as inf:
            with open(str(outfile), "a") as outf:
                reader = csv.reader(inf)
                next(reader, None)

                writer = csv.writer(outf)
                writer.writerow(output_cols)

                for row in tqdm(reader):
                    tweet = load_json(row[input_cols.index('tweet')])

                    if not tweet:
                        continue

                    tweet_id = str(tweet['id_str'])
                    posted_on = str(parse(tweet['created_at'], ignoretz=True))
                    user_id = str(tweet['user']['id_str'])
                    is_truncated = str(tweet['truncated'])

                    urls = get_tweet_urls(tweet)
                    if len(urls) > 0:
                        urls = str(urls)
                    else:
                        urls = None

                    if 'retweeted_status' in tweet:
                        retweeted_status = str(tweet['retweeted_status']['id_str'])
                    else:
                        retweeted_status = None

                    if 'quoted_status_id_str' in tweet:
                        quoted_status = str(tweet['quoted_status_id_str'])
                    else:
                        quoted_status = None

                    if tweet['in_reply_to_status_id_str']:
                        in_reply_to = str(tweet['in_reply_to_status_id_str'])
                    else:
                        in_reply_to = None

                    out = [tweet_id, posted_on, user_id, retweeted_status,
                           quoted_status, in_reply_to, urls, is_truncated]
                    out.append(row[input_cols.index('refetched')])
                    out.append(row[input_cols.index('error')])

                    writer.writerow(out)
