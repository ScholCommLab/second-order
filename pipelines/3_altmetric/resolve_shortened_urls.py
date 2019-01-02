#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

import sys
sys.path.append("../")

from _helpers import *

tqdm.pandas()


if __name__ == "__main__":
    root = Path('../../')
    Config = configparser.ConfigParser()
    Config.read(str(root / 'config.cnf'))

    altmetric_mentions = root / Config.get('input_files',  'altmetric')
    altmetric_urls = root / Config.get('output_files',  'altmetric_urls')

    queries = root / Config.get('input_files', 'queries')
    queries = load_queries(str(queries))

    am_news_mentions = pd.read_csv(altmetric_mentions)

    am_news_mentions = am_news_mentions.assign(resolve_url=np.nan, resolve_error=np.nan)
    am_news_mentions = am_news_mentions.assign(url=np.nan, clean_url=np.nan)

    # Resolve missing URLs
    session = requests.Session()
    for index, row in tqdm(am_news_mentions.iterrows(), total=len(am_news_mentions)):
        v = row['venue_short']
        terms = queries[queries.venue_short == v].relevant_terms.tolist()

        if 'moreover' in row['altmetric_url']:
            url, error = None, None

            try:
                resp = session.head(row['altmetric_url'], allow_redirects=True, timeout=10)
                url, error = resp.url, None
            except Exception as e:
                url, error = None, e

            am_news_mentions.loc[index, 'url'] = url
            am_news_mentions.loc[index, 'resolve_error'] = error
        else:
            am_news_mentions.loc[index, 'url'] = row['altmetric_url']

        am_news_mentions.loc[index, 'clean_url'] = clean_url(row['url'], row['venue_short'])
        am_news_mentions.loc[index, 'relevant'] = relevant_url(
            row['clean_url'], row['venue_short'], terms)

    am_news_mentions.to_csv(altmetric_urls)
