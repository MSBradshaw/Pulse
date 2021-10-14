import requests
import pandas as pd
from datetime import date
import json
import multiprocessing as mp
import typing
import time
import random


today = date.today()
todays_date = today.strftime("%Y-%m-%d")

# API Documenation http://api.biorxiv.org/
base_string = 'https://api.biorxiv.org/pub/2001-01-01/{}/{}'

# create a blank df and write it to the file with headers
info = {'biorxiv_doi': [], 'published_doi': [], 'preprint_title': [], 'preprint_category': [], 'preprint_date': [],
        'published_date': [], 'published_citation_count': []}
df = pd.DataFrame(info)
df.to_csv('Data/api_collected_data_{}.tsv'.format(todays_date), sep='\t', mode='w', header=True, index=False)

# initialize the cursor to start from the beginning
cursor = '0'
while True:
    print(cursor)
    resp = requests.get(base_string.format(todays_date, cursor))
    results = resp.json()['collection']
    # get the current number of articles
    count = resp.json()['messages'][0]['count']
    # increment the cursor by the number of articles in this batch
    cursor = str(int(resp.json()['messages'][0]['cursor']) + int(count) - 1)
    # create dict with teh same fields as the API endpoint
    info = {'biorxiv_doi': [], 'published_doi': [], 'preprint_title': [], 'preprint_category': [], 'preprint_date': [],
            'published_date': [], 'published_citation_count': []}
    # for each article
    for art in results:
        # add all the items to our info dict
        for key in info.keys():
            info[key].append(art[key].replace('\t', ' ').replace('\n', ' '))
    # convert the info dict to a dataframe
    df = pd.DataFrame(info)
    # append these results to file
    df.to_csv('Data/api_collected_data_{}.tsv'.format(todays_date), sep='\t', mode='a', header=False, index=False)
    # to stop it from being an infinite loop
    if int(cursor) >= (resp.json()['messages'][0]['total'] - 1):
        break

df = pd.read_csv('Data/api_collected_data_{}.tsv'.format(todays_date), sep='\t')

def get_single_article(_url: str) -> pd.DataFrame:
    """

    :param _url: url for a single article in the biorxiv API example:
    :return: pandas dataframe with information about the article
    """
    _article_info = {'doi': [], 'title': [], 'abstract': [], 'published': [], 'server': []}
    _resp = requests.get(_url)
    # add each item in the response to the dictionary of results
    for _key in _article_info.keys():
        # add the item to it's list and clean it of tabs and new lines
        try:
            try:
                _article_info[_key].append(_resp.json()['collection'][0][_key].replace('\t', ' ').replace('\n', ' '))
            except json.decoder.JSONDecodeError:
                print('Error with', _url)
                _article_info[_key].append('Missing')
        except IndexError:
            _article_info[_key].append('Missing')
    return pd.DataFrame(_article_info)


def collect_many_articles(params: typing.List) -> pd.DataFrame:
    """

    :param params: list with 2 items
        0.  todays date as a string
        1. list of string urls
    :return: pandas dataframe with all the article information
    """
    _todays_date = params[0]
    _urls = params[1]
    _df = None
    for _url in _urls:
        # a loop so there can be retries
        for i in range(2):
            try:
                # get the aricle info
                _temp_df = get_single_article(_url)
                break # you go the info, no need for a retry
            except requests.exceptions.ConnectionError:
                # continue on to the retry
                pass
        if _df is None:
            _df = _temp_df
        else:
            _df = pd.concat([_df, _temp_df])
    # sleep for 0-5 seconds randomly
    time.sleep((random.random() * 10 / 2))
    _df.to_csv('Data/api_collected_article_data_{}.tsv'.format(_todays_date), sep='\t', mode='a', header=False, index=False)
    return _df

details_url = 'https://api.biorxiv.org/details/biorxiv/{}'
chunk_size = 1000
dois = [details_url.format(x) for x in list(df.biorxiv_doi)]
param_lists = [[todays_date, dois[i:i + chunk_size]] for i in range(0, len(dois), chunk_size)]

start = time.time()
# create the file all processes will append to with column headers
article_info = {'doi': [], 'title': [], 'abstract': [], 'published': [], 'server': []}
pd.DataFrame(article_info).to_csv('Data/api_collected_article_data_{}.tsv'.format(todays_date), sep='\t', index=False)

pool = mp.Pool(processes=10)
pool.map(collect_many_articles, param_lists)
print('Duration: ' + str(time.time() - start))


articles_df = pd.read_csv('Data/api_collected_article_data_{}.tsv'.format(todays_date),sep='\t',header=None)

articles_df.columns = ['doi', 'title', 'abstract', 'published', 'server']

joint_df = pd.merge(articles_df, df, left_on='doi', right_on='biorxiv_doi')

joint_df.columns = ['doi', 'title', 'abstract', 'published_doi_2', 'server', 'biorxiv_doi',
       'published_doi', 'preprint_title', 'category', 'date',
       'published_date', 'published_citation_count']

keep_cols = ['doi', 'title', 'abstract', 'server', 'category', 'date']

keepers = joint_df[keep_cols]
keepers.to_csv('Data/processed_api_collected_article_data_{}.tsv'.format(todays_date), sep='\t', mode='w', index=False)