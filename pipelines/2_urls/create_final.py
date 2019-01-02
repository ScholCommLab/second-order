#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import json
from os import listdir
from os.path import isfile, join
from pathlib import Path
from pprint import pprint

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import urltools
from tqdm import tqdm

from _helpers import *

tqdm.pandas()

root = Path('../../')
Config = configparser.ConfigParser()
Config.read(str(root / 'config.cnf'))

queries = root / Config.get('input_files', 'queries')
tweet_dir = root / Config.get('output_files', 'tweets')
outdir = root / Config.get('output_files', 'twitter_urls')
input_files = Path("temp/")

# Load files from disk
files = list(input_files.glob("*.csv"))
queries = load_queries(str(queries))

for file in tqdm(files):
    if "expanded_url" in str(file):
        continue

    filename = str(file).split("/")[-1]

    # Load the original input tweet dataset
    tweets = load_tweets(str(tweet_dir / filename))

    # Load the temporary results
    temp_df = pd.read_csv(file,
                          na_values="None",
                          dtype={'tweet_id': str,
                                 'relevant_url': str,
                                 'expanded': bool},
                          parse_dates=['timestamp'])

    # add retweeted and quoted tweet ids to df
    temp_df = temp_df.merge(tweets[['retweeted_status', 'quoted_status']], left_index=True,
                            right_index=True, how="left", validate="one_to_one")

    # rtwts_with_urls = urls[(urls['retweet_id'].notnull()) & (
    #     urls['relevant_url'].notnull())].retweet_id
    # rtwts_wo_urls = urls[(urls['retweet_id'].notnull()) & (
    #     urls['relevant_url'].isnull())].retweet_id

    # # Copy relevant urls for retweeted statuses from original ones
    # for ix, row in urls[urls.retweet_id.isin(rtwts_wo_urls)].iterrows():
    #     rt_id = row['retweet_id']
    #     if rt_id in urls.index:
    #         if urls.loc[rt_id, 'relevant_url']:
    #             urls.loc[ix, 'retweet_url'] = urls.loc[rt_id, 'relevant_url']

    v = filename.split(" ")[0]
    terms = queries[queries.venue_short == v].relevant_terms.tolist()

    temp_df['cleaned_url'] = temp_df.relevant_url.map(lambda x: clean_url(x, v))
    temp_df['relevant'] = temp_df.cleaned_url.map(lambda x: relevant_url(x, v, terms))

    temp_df.to_csv(str(outdir / filename), header=True)
