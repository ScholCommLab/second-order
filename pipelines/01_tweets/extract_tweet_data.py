import csv
from pathlib import Path
import pandas as pd
from dateutil.parser import parse
from tqdm import tqdm

from helpers import *


if __name__ == "__main__":
    headers = ['tweet_id', 'posted_on', 'user_id', 'retweeted_status',
               'quoted_status', 'in_reply_to', 'urls', 'is_truncated']

    data_dir = Path("../../data/")
    input_dir = data_dir / "twitter_dump/"
    output_dir = data_dir / "tweets/"

    queries = load_queries("../../data/queries.csv")

    files = input_dir.glob("*.csv")
    for infile in files:
        outfile = output_dir / infile.name
        if outfile.exists():
            print("{} already exists. skipping".format(outfile))
            continue

        query_name = infile.name.split(".")[0]
        row_count = queries.loc[queries['query'] == query_name, "found_tweets"].iloc[0]

        print("Collecting {}".format(infile.name))
        with open(str(infile), "r") as inf:
            with open(str(outfile), "a") as outf:
                reader = csv.reader(inf)
                next(reader, None)

                writer = csv.writer(outf)
                writer.writerow(headers)

                for row in tqdm(reader, total=row_count):
                    tweet = load_json(row[1])

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

                    row = [tweet_id, posted_on, user_id, retweeted_status,
                           quoted_status, in_reply_to, urls, is_truncated]

                    writer.writerow(row)
