#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import operator
from os import listdir
from pathlib import Path

import pandas as pd
from tqdm import tqdm

tqdm.pandas()

data_dir = Path("../../data/")

all_tweets_dir = data_dir / "all_tweets"
selected_tweets_dir = data_dir / "refetched_tweets"
urls_dir = data_dir / "twitter_urls"
urls_cleaned_dir = data_dir / "twitter_urls_cleaned"

files = listdir(str(all_tweets_dir))

for file in tqdm(files):
    infile = pd.read_csv(str(all_tweets_dir / file), sep="\t",
                         names=['tweet_id', 'tweet'], index_col="tweet_id")
    infile.to_csv(str(all_tweets_dir / file).split(".txt")[0] + ".csv")
