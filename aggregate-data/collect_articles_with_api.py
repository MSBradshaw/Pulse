import requests
import pandas as pd
from datetime import date


today = date.today()
todays_date = today.strftime("%Y-%m-%d")

# API Documenation http://api.biorxiv.org/
base_string = 'https://api.biorxiv.org/pub/2001-01-01/{}/{}'

# create a blank df and write it to the file with headers
info = {'biorxiv_doi': [], 'published_doi': [], 'preprint_title': [], 'preprint_category': [], 'preprint_date': [], 'published_date': [], 'published_citation_count': []}
df = pd.DataFrame(info)
df.to_csv('api_collected_data_{}.tsv'.format(todays_date), sep='\t', mode='a', header=True, index=False)

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
    info = {'biorxiv_doi': [], 'published_doi': [], 'preprint_title': [], 'preprint_category': [], 'preprint_date': [], 'published_date': [], 'published_citation_count': []}
    # for each article
    for art in results:
        # add all the items to our info dict
        for key in info.keys():
            info[key].append(art[key].replace('\t', ' ').replace('\n', ' '))
    # convert the info dict to a dataframe
    df = pd.DataFrame(info)
    # append these results to file
    df.to_csv('api_collected_data_{}.tsv'.format(todays_date), sep='\t', mode='a', header=False, index=False)