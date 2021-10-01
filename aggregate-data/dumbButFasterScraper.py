import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import datetime
import multiprocessing as mp
import time
import os
from databaseaccess import DataBaseAccess
from databaseaccess import date_to_unix_time
from buildSqlTable import add_articles, clean_for_sql

VERBOSE = True


def print_verbose(string):
    global VERBOSE
    if VERBOSE:
        print(string)


def log_error(logfile, error_type, url):
    d = datetime.datetime.today()
    date = d.strftime("%d-%B-%Y %H:%M:%S")
    f = open(logfile, "a")
    f.write(date + "\t" + error_type + "\t" + url + '\n')
    f.close()


"""
Given a url for a single article on Biorxiv
Return a map of the date, title, authors, abstract and url
Returns -1 if errors occurred
"""


def get_single_article(url, logfile):
    try:
        html = requests.get(url, verify=False).text
        soup = BeautifulSoup(html, 'html.parser')
    except AttributeError:
        log_error(logfile, 'Failure: cannot access article html', url)
        return -1

    # find the date
    try:
        date = ''
        soup.find_all("div", {"class": "pane-content"})
        for foo in soup.find_all('div', attrs={'class': 'pane-1'}):
            bar = foo.find('div', attrs={'class': 'pane-content'})
            date = bar.text
            date = date.replace('Posted', '')
            date = date.replace('.', '')
            date = re.sub('^\\s*(\\w)', '\\g<1>', date)
            date = re.sub('\\s*$', '', date)
        print_verbose('|' + date + '|')
    except AttributeError:
        log_error(logfile, 'Failure: article has no date', url)
        return -1

    # get the abstract
    found = False
    id_base = 'p-'
    for i in range(2, 20):
        abstract = soup.find("p", {"id": id_base + str(i)})
        if abstract is not None:
            abstract = abstract.text
            found = True
            break
    if found:
        abstract = re.sub('\n+', ' ', abstract)
        print_verbose(abstract)
    else:
        log_error(logfile, 'Failure: article has no abstract p-2 - p-19', url)
        return -1

    # get the title
    try:
        title = soup.find("h1", {"id": "page-title"}).text
        print_verbose(title)
    except AttributeError:
        log_error(logfile, 'Failure: article has no title', url)
        return -1

    # get the authors
    try:
        authors = soup.find("span", {"class": "highwire-citation-authors"}).text
        authors = authors.replace('View ORCID Profile', '')
        # remove leading space
        authors = re.sub('^\\s+', '', authors)
        # remove extra space
        authors = re.sub('\\s+', ' ', authors)
        print_verbose(authors)
    except AttributeError:
        log_error(logfile, 'Failure: article has no authors', url)
        return -1

    result_map = {
        'date': date,
        'title': title,
        'authors': authors,
        'abstract': abstract,
        'url': url
    }
    return result_map


# print_verbose(get_single_article('https://www.biorxiv.org/content/10.1101/2020.03.18.996538v1','aggregate-data/logs/bioinformatics.txt'))

"""
Give the url for a biorxiv list page, an outputfile path and a logfile path
Get the relevant information from each article
When the page is complete, append content to output file
Append errors or successes to logfile
"""


def get_page(url, outputfile, logfile, collection_name, is_update=False):
    print_verbose('Doing Page: ' + url)
    for i in range(1,3):
        if i > 1:
            print('Attempt:', str(i))
        html = requests.get(url, verify=False).text
        soup = BeautifulSoup(html, 'html.parser')
        article_list = soup.find_all('a', {'class': 'highwire-cite-linked-title'}, href=True)

        base_biorxiv_url = 'https://www.biorxiv.org'
        output = pd.DataFrame()
        for a in article_list:
            dictionary = get_single_article(base_biorxiv_url + a['href'], logfile)
            # check if there was an error in getting the article
            if dictionary is not -1:
                output = output.append(dictionary, ignore_index=True)

        # if there is no output, reattempt it
        if output.shape[0] == 0:
            continue
        output = output[['date', 'title', 'authors', 'url', 'abstract']]
        output['date'] = [date_to_unix_time(x) for x in list(output['date'])]
        if not is_update:
            # go ahead and save all the information to the csv
            output.to_csv(outputfile, mode='a', header=False)
            # not an error but log the success
            log_error(logfile, 'Complete List Page', url)
            # return True so the calling function knows to continue scraping
            return True
        # add the subcategory to the DataFrame in the last column position
        output['subcategory'] = collection_name
        # check if it is already in the DB
        db = DataBaseAccess()
        # set it to be using UTF-8 to allow for special characters
        db.cursor.execute("SET collation_connection = 'utf8_general_ci';")
        statement_template = "SELECT article_id FROM articles WHERE title='{the_title}' AND publish_date={the_date};"
        articles_to_save = output.copy()
        indexes_to_drop = []
        for i in range(output.shape[0]):
            title = clean_for_sql(output.iloc[i, :]['title'])
            date = output.iloc[i, :]['date']
            sql_statement = statement_template.format(the_title=title, the_date=date)
            print_verbose(sql_statement)
            db.cursor.execute(sql_statement)
            results = db.cursor.fetchall()
            if len(results) < 1:
                # the articles does not exist
                pass
            else:
                # the article is already found
                indexes_to_drop.append(i)
                print_verbose('\tArticle already Exists')

                # add it to the db

        # drop the articles that already exist
        print_verbose('\tDropping:' + str(indexes_to_drop))
        articles_to_save = articles_to_save.drop(articles_to_save.index[indexes_to_drop])

        # add the good articles to the db
        if articles_to_save.shape[0] > 0:
            print_verbose('\tAdding')
            print_verbose(articles_to_save)
            add_articles(db.connection, articles_to_save)
            # remove the sub category column before adding it to the csv as this is not part of the original schema
            articles_to_save = articles_to_save.drop(columns=['subcategory'])
            # append new articles to the csv
            articles_to_save.to_csv(outputfile, mode='a', header=False)
            # not an error but log the success
            log_error(logfile, 'Complete List Page', url)
            if articles_to_save.shape[0] == output.shape[0]:
                # if all the articles were new, return true so the next page is collected too
                return True
            else:
                # if only some of the articles were new, return false so the next page is not gathered
                return False
        else:
            # return False as is there were no new articles
            return False


# get_page('https://www.biorxiv.org/collection/bioinformatics?page=0','aggregate-data/CSVs/bioinformatics.csv','aggregate-data/logs/bioinformatics.txt')


"""
Function used to call get_page in a async multiprocess way
"""

def start_func(params):
    start = params[0]
    end = params[1]
    url_base = params[2]
    outfile = params[3]
    logfile = params[4]
    collection_name = params[5]
    for i in range(start, end):
        url = url_base + '?page=' + str(i)
        get_page(url, outfile, logfile, collection_name, False)
    return "Done" + str(params[0]) + " " + str(params[1])


"""
Given the url for a biorxiv list page, an outputfile path, a logfile path and the name of the collection
Collect all articles from the collection in parallel, save them to the outputfile CSV, not does add to DB
"""


def get_collection(url, outputfile, logfile, collection_name):
    start = time.time()
    # find the index of the last page
    html = requests.get(url, verify=False).text
    soup = BeautifulSoup(html, 'html.parser')
    last = soup.find('li', {'class': 'pager-last last odd'})
    last = int(last.text)
    # make list of parameter lists for parallel runs, chunking by 25s
    param_lists = [
        [i, i + 24, url, outputfile, logfile, collection_name] if i + 24 < last else [i, last, url, outputfile, logfile, collection_name]
        for i
        in range(0, last, 25)]
    pool = mp.Pool(processes=12)
    pool.map(start_func, param_lists)
    print('Duration: ' + str(time.time() - start))


# get_collection('https://www.biorxiv.org/collection/bioinformatics', 'aggregate-data/CSVs/bioinformatics.csv',
#                'aggregate-data/logs/bioinformatics.txt')

"""
Given:
col_url - collection url example 'https://www.biorxiv.org/collection/bioinformatics'
outputfile - path to csv where new records should be saved
logfile - path to log file where diagnostics messages should be written
collection_name - name of the collection, should match the end of the col_url. Will be used to identify this article's collection in the db
Loop though all pages in the collection. Add new articles to the DB and save them to the csv file.
Once articles are encountered that are already in the DB: return from the function.
"""


def update_collection(col_url, outputfile, logfile, collection_name):
    print_verbose('Updating Collection: ' + collection_name)
    # find the index of the last page
    html = requests.get(col_url, verify=False).text
    soup = BeautifulSoup(html, 'html.parser')
    last = soup.find('li', {'class': 'pager-last last odd'})
    last = int(last.text)
    for i in range(0, last):
        url = col_url + '?page=' + str(i)
        print_verbose('\t' + collection_name + ' subpage ' + str(i))
        # gather the information for each page
        should_continue = get_page(url, outputfile, logfile, collection_name, True)
        # if you have reached previously contained data, stop collecting data
        if not should_continue:
            print_verbose('Finished Updating: ' + collection_name)
            return
    print_verbose('Collected All of: ' + collection_name)


"""
Given:
update - boolean variable for if this function should update or do a full crawl
True: call the update collection function for each of the collection (subcategories) and retrieves only articles not already in DB
False: calls get_collection for each category, gets all articles but does NOT save them to the DB, just to a CSV
"""


def get_all_collections(update=False):
    requests.packages.urllib3.disable_warnings()
    base_url = 'https://www.biorxiv.org'
    # get the name of all collections
    html = requests.get(base_url, verify=False).text
    soup = BeautifulSoup(html, 'html.parser')
    collections = soup.find_all('span', {'class': 'field-content'})
    collection_names = []
    for c in collections:
        a = c.find('a', href=True)
        collection_names.append(a['href'].replace('/collection/', ''))

    for col in collection_names:
        print('Current Collection: ' + col)
        url = 'https://www.biorxiv.org/collection/' + col
        outfile = 'aggregate-data/CSVs/' + col + '.csv'
        logfile = 'aggregate-data/logs/' + col + '.txt'
        # if os.path.exists(outfile):
        #     print('Skipping')
        #     continue

        open(logfile, 'a').close()
        open(outfile, 'a').close()
        if update:
            update_collection(url, outfile, logfile, col)
        else:
            get_collection(url, outfile, logfile, col)


# get_all_collections()
#
# get_page('https://www.biorxiv.org/collection/bioengineering?page=7','delete.csv','delete.log','bioengineering', True)
# update_collection('https://www.biorxiv.org/collection/bioengineering', 'delete.csv', 'delete.log', 'bioengineering')

get_all_collections(False)
