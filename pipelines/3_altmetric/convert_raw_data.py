#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
from pathlib import Path

import pandas as pd

import sys
sys.path.append("../")

from _helpers import *


def assign_venue(x, venues):
    for venue in venues:
        if venue in x:
            return venue
    return None

if __name__ == "__main__":
    root = Path('../../')
    Config = configparser.ConfigParser()
    Config.read(str(root / 'config.cnf'))

    raw_altmetric = root / Config.get('input_files', 'altmetric_raw')
    altmetric = root / Config.get('input_files',  'altmetric')

    queries = root / Config.get('input_files', 'queries')
    queries = load_queries(str(queries))

    # Load Altmetric Excel sheet and save cleaned version
    am_news_mentions = pd.read_excel(raw_altmetric, sheet_name=1,
                                     index_col="Altmetric_ID", parse_dates=['Posted_On'])
    am_news_mentions.index.name = 'altmetric_id'
    am_news_mentions.rename(columns={'Author_name': 'venue_name',
                                     'Url': 'altmetric_url',
                                     'Author_Url': 'venue_url',
                                     'Posted_On': 'posted_on'}, inplace=True)

    # Assign short labels for venues
    short_venues = queries['venue_short'].unique().tolist()
    am_news_mentions['venue_short'] = am_news_mentions['venue_url'].map(
        lambda x: assign_venue(x, short_venues))
    am_news_mentions.to_csv(altmetric)
