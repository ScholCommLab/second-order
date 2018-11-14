import csv
import json
from pathlib import Path
import pandas as pd
from dateutil.parser import parse
from tqdm import tqdm

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


def load_queries(file):
    '''
    Load file that contains information about the search queries
    '''
    queries = pd.read_csv(file, index_col="id")
    return queries


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

        print("Collecting {}".format(infile.name))
        with open(str(infile), "r") as inf:
            with open(str(outfile), "a") as outf:
                reader = csv.reader(inf)
                next(reader, None)
                
                writer = csv.writer(outf)
                writer.writerow(headers)

                query_name = infile.name.split(".")[0]
                row_count = queries.loc[queries['query'] == query_name, "found_tweets"].iloc[0]

                for row in tqdm(reader, total=row_count):
                    tweet = load_json(row[1])

                    if not tweet:
                        continue

                    tweet_id = tweet['id_str']
                    posted_on = parse(tweet['created_at'], ignoretz=True)
                    user_id = tweet['user']['id_str']
                    if 'retweeted_status' in tweet:
                        retweeted_status = tweet['retweeted_status']['id_str']
                    else:
                        retweeted_status = None
                    if tweet['is_quote_status']:
                        quoted_status = tweet['quoted_status_id_str']
                    else:
                        quoted_status = None
                    in_reply_to = tweet['in_reply_to_status_id_str']
                    urls = get_tweet_urls(tweet)
                    is_truncated = tweet['truncated']

                    row = [str(tweet_id), str(posted_on), str(user_id), str(retweeted_status),
                           str(quoted_status), str(in_reply_to), str(urls), is_truncated]

                    writer.writerow(row)
