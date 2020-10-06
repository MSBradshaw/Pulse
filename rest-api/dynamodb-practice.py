import boto3
import pandas as pd
import pandas as pd
import re
import time
import datetime
from os import listdir

VERBOSE = True

"""
Give a string (or something else)
print_verbose the string if the VERBOSE global is true
"""


def print_verbose(string):
    global VERBOSE
    if VERBOSE:
        print(string)


def date_to_unix_time(date):
    if date == 'Fail':
        return -1
    dt = datetime.datetime.strptime(date, '%B %d, %Y')
    return int(time.mktime(dt.timetuple()))


"""
Give the path to a file
Read it in as a pd DataFrame with out headers
Give column names to the df
Removed the index column
Drops duplicate entries
Returns a pandas DataFrame
"""


def load_data(filepath):
    df = pd.read_csv(filepath, header=None)
    df.columns = ['index', 'publish_date', 'title', 'authors', 'url', 'abstract']
    df['publish_date'] = [date_to_unix_time(x) for x in df['publish_date']]
    df = df.drop(['index'], axis=1)
    df = df.drop_duplicates()
    # remove rows that have missing data
    df = df[pd.notnull(df['abstract'])]
    df = df[pd.notnull(df['publish_date'])]
    df = df[pd.notnull(df['title'])]
    df = df[pd.notnull(df['authors'])]
    df = df[pd.notnull(df['url'])]
    return df


"""
Given a df and a DynamoDB client
Add each article in the df to the db
"""


def add_articles(df, table):
    myl = df.T.to_dict().values()
    counter = 0
    for student in myl:
        if counter % 1000 == 0:
            print(counter)
        counter += 1
        table.put_item(Item=student)
    # with table.batch_writer() as batch:
    #     for i in range(df.shape[0]):
    #         if i % 1000 == 0:
    #             print('\t' + str(i))
    #         batch.put_item(
    #             Item={
    #                 'title': df.iloc[i, 1],
    #                 'publish_date': int(df.iloc[i, 0]),
    #                 'authors': df.iloc[i, 2],
    #                 'abstract': df.iloc[i, 4],
    #                 'url': df.iloc[i, 3],
    #                 'subcategory': df.iloc[i, 5],
    #             }
    #         )


ddb = boto3.resource('dynamodb')
client = boto3.client('dynamodb')

# delete the old table
try:
    print('Deleting old table')
    # client.delete_table(TableName='articles')
    # waiter = client.get_waiter('table_not_exists')
    # waiter.wait(TableName='articles')
except client.exceptions.ResourceNotFoundException:
    pass

# Create the DynamoDB table.
try:
    table = client.create_table(
        TableName='articles',
        KeySchema=[
            {
                'AttributeName': 'title',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'publish_date',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'publish_date',
                'AttributeType': 'N'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    print('Creating articles table...')
    waiter = client.get_waiter('table_exists')
    waiter.wait(TableName='articles')
except client.exceptions.ResourceInUseException:
    pass

# add dumby values to the table
table = ddb.Table('articles')

start = time.time()
for f in listdir('aggregate-data/CSVs/'):
    subcat = re.sub('\\.csv', '', f)
    print(subcat)
    d = load_data('aggregate-data/CSVs/' + f)
    d['subcategory'] = subcat
    add_articles(d, table)

print('Duration: ' + str(time.time() - start))