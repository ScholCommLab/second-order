import pandas as pd
import requests
from ratelimit import limits, sleep_and_retry

try:  # for notebooks
    get_ipython
    from tqdm._tqdm_notebook import tqdm_notebook as tqdm
except:  # for commandline
    from tqdm import tqdm
tqdm.pandas()


def resolve_url(url, session, timeout=10):
    try:
        resp = session.head(url, allow_redirects=True, timeout=timeout)
        return resp.url, None
    except Exception as e:
        return None, e


def choose_url(row):
    if 'moreover' in row['altmetric_url']:
        return row['resolve_url']
    else:
        return row['altmetric_url']


news_short_altmetric = {
    'bostonglobe': ['The Boston Globe'],
    'chicago': ['Chicago Sun-Times'],
    'foxnews': ['FOX News'],
    'guardian': ['The Guardian'],
    'iflscience': [],
    'latimes': ['LA Times'],
    'nytimes': ['New York Times'],
    'sfchronicle': ['San Francisco Chronicle'],
    'slate': ['Slate Magazine', 'Slate France'],
    'theglobeandmail': ['The Globe and Mail'],
    'washingtonpost': ['Washington Post'],
    'wired': ['Wired.it', 'Wired.com', 'Wired.co.uk']
}

news_altmetric_short = {}
for a, ss in news_short_altmetric.items():
    for s in ss:
        news_altmetric_short[s] = a


# Load Altmetric Excel sheet and save cleaned version
am_news_mentions = pd.read_excel("NewsMentions-AltmetricOct2017.xlsx",
                                 sheet_name=1, index_col="Altmetric_ID", parse_dates=['Posted_On'])
am_news_mentions.index.name = 'altmetric_id'
am_news_mentions.rename(columns={'Author_name': 'venue_name',
                                 'Url': 'altmetric_url',
                                 'Author_Url': 'venue_url',
                                 'Posted_On': 'posted_on'}, inplace=True)
am_news_mentions['venue_short'] = am_news_mentions['venue_name'].map(
    lambda x: news_altmetric_short[x])
am_news_mentions.to_csv("am_news_mentions.csv")

# Create altmetric URLs dataframe
am_news_urls = am_news_mentions.drop_duplicates()
am_news_urls = am_news_urls.reset_index()
del am_news_urls['altmetric_id']
am_news_urls = am_news_urls.assign(resolve_url=np.nan, resolve_error=np.nan)

# Resolve missing URLs
session = requests.Session()
for index, row in tqdm(am_news_urls.iterrows(), total=len(am_news_urls)):
    if 'moreover' in row['Url']:
        url, e = resolve_url(row['Url'], session)
        am_news_urls.loc[row.name, 'resolved_url'] = url
        am_news_urls.loc[row.name, 'resolve_error'] = e

# merge resolved and altmetric urls
am_news_urls['url'] = am_news_urls.apply(choose_url, axis=1)
am_news_urls.to_csv("am_news_urls.csv")
