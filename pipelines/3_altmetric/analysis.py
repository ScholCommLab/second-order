#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from os import listdir
from os.path import isfile, join
from pathlib import Path
from pprint import pprint

import numpy as np
import pandas as pd
import urltools
from tqdm import tqdm

tqdm.pandas()

data_dir = Path("../../data/")
path_final = data_dir / "twitter_urls_cleaned"
path_altmetric = data_dir / "altmetric_urls"

files = listdir(str(path_final))

am_news_urls = pd.read_csv(str(path_altmetric / "altmetric_urls_relevant.csv"), index_col="id")

am_urls = am_news_urls[(am_news_urls['clean_url'].notnull()) & (
    am_news_urls.relevant == True)].groupby("venue_short")['clean_url'].unique()

urls_per_file = {}
for file in tqdm(files):
    df = load_urls(path_final / file)

    v = file.split(" ")[0]
    df = df.assign(venue_short=v)

    df['url'] = df.apply(merge_urls, axis=1)
    df['clean_url'] = df.apply(clean_url, axis=1)

    for vals in df[['clean_url', 'venue_short']].itertuples():
        df.loc[vals[0], 'relevant'] = relevant_url(vals[1], vals[2])
    urls_per_file[v] = df

urls_per_venue = {}
for v, df in tqdm(urls_per_file.items(), total=len(urls_per_file)):
    l = df[df.relevant].clean_url.tolist()

    if v in urls_per_venue:
        urls_per_venue[v] = urls_per_venue[v].union(l)
    else:
        urls_per_venue[v] = set(l)


df = pd.DataFrame(columns=['Twitter', 'Altmetric', 'both'])
for short in urls_per_venue.keys():
    try:
        tw = len(urls_per_venue[short])
        if short in am_urls:
            am = len(am_urls[short])
            both = len(urls_per_venue[short].intersection(am_urls[short]))
        else:
            am = 0
            both = 0
        df.loc[short] = [tw, am, both]
    except:
        raise

df.index.name = "venue"
df.to_csv("results.csv")
