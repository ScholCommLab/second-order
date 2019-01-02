#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
import json
import sys
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

sys.path.append("../")
from _helpers import *

tqdm.pandas()


if __name__ == "__main__":
    root = Path('../../')
    Config = configparser.ConfigParser()
    Config.read(str(root / 'config.cnf'))

    queries = root / Config.get('input_files', 'queries')
    tweet_dir = root / Config.get('output_files', 'tweets')
    twitter_urls = root / Config.get('output_files', 'twitter_urls')

    input_files = Path("temp/")

    # Load files from disk
    files = list(input_files.glob("*.csv"))
    queries = load_queries(str(queries))

    dfs = []
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

        v = filename.split(" ")[0]
        terms = queries[queries.venue_short == v].relevant_terms.tolist()

        temp_df['cleaned_url'] = temp_df.relevant_url.map(lambda x: clean_url(x, v))
        temp_df['relevant'] = temp_df.cleaned_url.map(lambda x: relevant_url(x, v, terms))

        dfs.append(temp_df)

    urls = pd.concat(dfs)
    urls.reset_index(drop=True)
    urls.index.name = "id"
    urls.to_csv(twitter_urls)
