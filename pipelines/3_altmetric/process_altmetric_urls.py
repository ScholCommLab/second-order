#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
from os import listdir
from os.path import isfile, join
import json
from pathlib import Path
from pprint import pprint
import numpy as np

import urltools

from tqdm import tqdm
tqdm.pandas()

if __name__ == "__main__":
    data_dir = Path("../../data/")
    path_final = data_dir / "twitter_urls_cleaned"
    path_altmetric = data_dir / "altmetric_urls"

    am_news_mentions = pd.read_csv(
        str(path_altmetric / "altmetric_news_mentions.csv"), index_col="altmetric_id")
    am_news_urls = pd.read_csv(str(path_altmetric / "altmetric_urls.csv"), index_col="id")

    am_news_urls['clean_url'] = am_news_urls.apply(clean_url, axis=1)
    am_news_urls = am_news_urls.assign(relevant=np.nan)

    for vals in tqdm(am_news_urls[['clean_url', 'venue_short']].itertuples(), total=len(am_news_urls)):
        am_news_urls.loc[vals[0], 'relevant'] = relevant_url(vals[1], vals[2])

    am_news_urls.to_csv(str(path_altmetric / "altmetric_urls_relevant.csv"))
