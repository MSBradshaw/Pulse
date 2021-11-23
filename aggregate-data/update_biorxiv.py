import requests
import pandas as pd
from datetime import date
import json
import multiprocessing as mp
import typing
import time
import random
from databaseaccess import DataBaseAccess, unxi_to_date_month
import buildSqlTable

def collect_all_article_meta_data(end_date: str, start_date: str) -> pd.DataFrame:
    """
    Collect all article meta data from biorxiv, saves it in the file Data/api_collected_data_{todays_date}.tsv
    :param end_date: date to stop collecting at date in "%Y-%m-%d" format (you probabaly just want today's date)
    :param start_date: date to start collecting from in "%Y-%m-%d" format
    :return: pd.DataFrame of all article's meta data
    """
    # API Documentation http://api.biorxiv.org/
    base_string = 'https://api.biorxiv.org/details/biorxiv/{}/{}/{}'

    # create a blank df and write it to the file with headers
    info = {'doi': [], 'title': [], 'authors': [], 'author_corresponding': [], 'author_corresponding_institution': [],
            'date': [], 'version': [], 'type': [], 'license': [], 'category': [], 'jatsxml': [], 'abstract': [],
            'published': [], 'server': []}
    df = pd.DataFrame(info)
    df.to_csv('Data/api_collected_data_{}.tsv'.format(end_date), sep='\t', mode='w', header=True, index=False)

    # initialize the cursor to start from the beginning
    cursor = '0'
    previous_cursor = '-1'
    stop = False
    while True:
        # loop to allow second attempts at gather the information
        for i in range(2):
            try:
                resp = requests.get(base_string.format(start_date, end_date, cursor))
                print(base_string.format(start_date, end_date, cursor))
                results = resp.json()['collection']
                # get the current number of articles
                count = resp.json()['messages'][0]['count']
                # increment the cursor by the number of articles in this batch
                cursor = str(int(resp.json()['messages'][0]['cursor']) + int(count) - 1)
                # create dict with teh same fields as the API endpoint
                info = {'doi': [], 'title': [], 'authors': [], 'author_corresponding': [],
                        'author_corresponding_institution': [], 'date': [], 'version': [],
                        'type': [], 'license': [], 'category': [], 'jatsxml': [], 'abstract': [],
                        'published': [], 'server': []}
                # for each article
                for art in results:
                    # add all the items to our info dict
                    for key in info.keys():
                        info[key].append(art[key].replace('\t', ' ').replace('\n', ' '))
                # convert the info dict to a dataframe
                df = pd.DataFrame(info)
                # append these results to file
                if str(cursor) != str(previous_cursor):
                    df.to_csv('Data/api_collected_data_{}.tsv'.format(end_date), sep='\t', mode='a', header=False, index=False)
                    previous_cursor = str(cursor)
                else:
                    # set stop to true, break out of the retry and end the collection process
                    stop = True
                    break
                # to stop it from being an infinite loop
                print()
                print(cursor)
                print(resp.json()['messages'][0]['total'])
                break
            except requests.exceptions.ConnectionError:
                print('Connection Error, trying again')
        if int(cursor) >= (resp.json()['messages'][0]['total'] - 1) or stop:
            break
    df = pd.read_csv('Data/api_collected_data_{}.tsv'.format(end_date), sep='\t')
    return df


def collect_all_articles(todays_date: str, article_meta_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Based on the DOI's found in article_meta_data_df collect the more specific article information (like the abstract),
    save it to a file (Data/api_collected_article_data_{}.tsv) and return it as a pd.DataFrame
    :param todays_date: today's date in "%Y-%m-%d" format
    :param article_meta_data_df: pd.DataFrame from collect_all_article_meta_data
    :return: pd.DataFrame with information about individual articles
    """
    details_url = 'https://api.biorxiv.org/details/biorxiv/{}'
    chunk_size = 1000
    dois = [details_url.format(x) for x in list(article_meta_data_df.doi)]
    param_lists = [[todays_date, dois[i:i + chunk_size]] for i in range(0, len(dois), chunk_size)]

    start = time.time()
    # create the file all processes will append to with column headers
    article_info = {'doi': [], 'title': [], 'abstract': [], 'published': [], 'server': []}
    pd.DataFrame(article_info).to_csv('Data/api_collected_article_data_{}.tsv'.format(todays_date), sep='\t',
                                      index=False)

    pool = mp.Pool(processes=10)
    pool.map(collect_many_articles, param_lists)
    print('Duration: ' + str(time.time() - start))

    articles_df = pd.read_csv('Data/api_collected_article_data_{}.tsv'.format(todays_date), sep='\t', header=None)

    articles_df.columns = ['doi', 'title', 'abstract', 'published', 'server']
    return articles_df


def collect_many_articles(params: typing.List) -> pd.DataFrame:
    """
    function intended to be called as part of an mp.Pool to collect articles from biorxiv in parallel
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
                break  # you go the info, no need for a retry
            except requests.exceptions.ConnectionError:
                # continue on to the retry
                pass
        if _df is None:
            _df = _temp_df
        else:
            _df = pd.concat([_df, _temp_df])
    # sleep for 0-5 seconds randomly
    time.sleep((random.random() * 10 / 2))
    _df.to_csv('Data/api_collected_article_data_{}.tsv'.format(_todays_date), sep='\t', mode='a', header=False,
               index=False)
    return _df

def get_single_article(_url: str) -> pd.DataFrame:
    """
    collects information about a single biorxiv article from the api and returns it as a pd.DataFrame
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


def aggregate_and_clean_article_data(todays_date: str, articles_df: pd.DataFrame,
                                     article_meta_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Take the meta data df and article information df, combines them, save the combination as
    Data/processed_api_collected_article_data_{todays_date}.tsv and returns it as a pd/DataFrame
    :param todays_date: today's date in "%Y-%m-%d" format
    :param articles_df: pd.DataFrame from collect_all_article_meta_data
    :param article_meta_data_df: pd.DataFrame with information about individual articles from collect_all_articles
    :return: pd.DataFrame with only the information about articles needed in the Pulse MySQL database
    """
    joint_df = pd.merge(articles_df, article_meta_data_df, left_on='doi', right_on='doi')

    # joint_df.columns = ['doi', 'title', 'abstract', 'published_doi_2', 'server', 'doi2',
    #                     'published_doi', 'preprint_title', 'category', 'date',
    #                     'published_date', 'published_citation_count']
    joint_df.columns =['doi', 'title', 'abstract', 'published', 'server', 'title_y',
     'authors', 'author_corresponding', 'author_corresponding_institution',
     'date', 'version', 'type', 'license', 'category', 'jatsxml',
     'abstract_y', 'published_y', 'server_y']
    # print(a)
    keep_cols = ['doi', 'title', 'abstract', 'server', 'category', 'date']

    keepers = joint_df[keep_cols]
    keepers.to_csv('Data/processed_api_collected_article_data_{}.tsv'.format(todays_date), sep='\t', mode='w',
                   index=False)
    return keepers

def remove_duplicates():
    da = DataBaseAccess()
    # get the duplicate article ids
    com="SELECT article_id FROM articles GROUP BY publish_date, doi HAVING COUNT(*) > 1"
    num_res = da.cursor.execute(com)
    result = da.cursor.fetchall()
    remove_com = "DELETE FROM articles WHERE article_id = {}"
    for r in result:
        print(r)
        da.cursor.execute(remove_com.format(str(r[0])))
    da.cursor.close()
    da.connection.commit()


def update():
    """
    Run this function to update the pulse database with the biorxiv information.
    It will find the most recent date in in pulse, then add gather everything from biorxiv added since that date
    :return: None
    """
    pass
    # get the most recent date
    da = DataBaseAccess()
    # get the most recent date in the DB
    da.cursor.execute('SELECT MAX(publish_date) FROM articles')
    last_date = unxi_to_date_month(da.cursor.fetchone()[0])
    last_date = '2021-10-01'
    today = date.today()
    todays_date = today.strftime("%Y-%m-%d")

    # gather the article meta information
    article_meta = collect_all_article_meta_data(todays_date, last_date)
    articles_df = collect_all_articles(todays_date, article_meta)
    final_df = aggregate_and_clean_article_data(todays_date, articles_df, article_meta)
    final_df['date'] = [buildSqlTable.date_to_unix_time(x) for x in list(final_df['date'])]
    # d = buildSqlTable.load_biorxiv_data('Data/processed_api_collected_article_data_{}.tsv'.format(todays_date))
    start = time.time()
    buildSqlTable.add_articles(da.connection, final_df)
    print('Time Spent Adding: ' + str(time.time() - start))

    print(da.cursor.execute("SELECT article_id FROM articles"))
    result = da.cursor.fetchall()
    remove_duplicates()
    print('Total Articles: ' + str(len(result)))


if __name__ == "__main__":
    print('Updating Pulse with most recent Biorxiv')
    update()