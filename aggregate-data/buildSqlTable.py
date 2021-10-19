import pandas as pd
import re
import time
import datetime
from os import listdir
import pymysql
from datetime import date

VERBOSE = False


def clean_for_sql(string):
    string = string.replace("'", "''")
    string = string.replace('"', '""')
    return string


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
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return int(time.mktime(dt.timetuple()))


"""
Give the path to a file
Read it in as a pd DataFrame with out headers
Give column names to the df
Removed the index column
Drops duplicate entries
Returns a pandas DataFrame
"""


def load_biorxiv_data(filepath):

    df = pd.read_csv(filepath, sep='\t')
    print(df.shape)
    # df.columns = ['index', 'publish_date', 'title', 'authors', 'url', 'abstract']

    df.columns = ['doi', 'title', 'authors', 'author_corresponding', 'author_corresponding_institution', 'publish_date',
                  'version', 'type', 'license', 'category', 'jatsxml', 'abstract', 'published', 'server']

    #df.columns = ['doi', 'title', 'abstract', 'server', 'category', 'publish_date']
    # set a subset of columns
    df = df[['doi', 'title', 'abstract', 'server', 'category', 'publish_date']]

    df['publish_date'] = [date_to_unix_time(x) for x in df['publish_date']]
    # df = df.drop(['index'], axis=1)
    df = df.drop_duplicates()
    # remove rows that have missing data
    df = df[pd.notnull(df['doi'])]
    df = df[pd.notnull(df['title'])]
    df = df[pd.notnull(df['abstract'])]
    df = df[pd.notnull(df['server'])]
    df = df[pd.notnull(df['category'])]
    df = df[pd.notnull(df['publish_date'])]
    print(df.shape)
    return df


"""
Give a string
Get all unique words from the string
"""


def get_unique_words(string):
    # convert to lower case
    string = string.lower()
    # split on space
    words = list(set(string.split(' ')))
    # remove punctuation from end of words
    words = [re.sub('(\\w+)([.;?,!):])', '\\g<1>', x) if re.match('\\w+([.;?,!):])', x) else x for x in words]
    # remove numbers
    words = [x for x in words if not x.isdigit()]
    return words


"""
Give the path to a sqlite database
Open a connection to the db
Create tables if they do not exist
Return the sqlite3 connection and a cursor to the db 
"""


def open_db():
    info = []
    for line in open('not_important_info.txt'):
        info.append(line.strip())
    host = info[0]
    port = int(info[4])
    user = info[1]
    password = info[2]
    db = info[3]

    connection = pymysql.connect(host, user=user, port=port, passwd=password, db=db)

    return connection


"""
Given a conneciton
create a cursor
Clear old data, create the new table
close cursor and commit
"""


def create_db(connection):
    cursor = connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS articles')

    create_articles_statement = """CREATE TABLE IF NOT EXISTS articles (
        article_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        publish_date INT NOT NULL,
        title TEXT NOT NULL,
        doi TEXT NOT NULL,
        abstract TEXT NOT NULL,
        server TEXT NOT NULL,
        category TEXT NOT NULL);
        """
    cursor.execute(create_articles_statement)

    cursor.close()
    connection.commit()


"""
Given an sqlite3 connection and a pandas DataFrame
Give column names to the DataFrame
Adds whole DataFrame to the articles table
"""


def add_articles(connection, df):
    cursor = connection.cursor()
    print_verbose(df.columns)
    insert_template = "INSERT INTO articles (publish_date, title, doi, abstract, server, category) VALUES " \
                      "('{publish_date}', '{title}', '{doi}', '{abstract}', '{server}', '{category}');"
    for i in range(df.shape[0]):
        try:
            if i % 1000 == 0:
                print('Loading')
                print(i)
                print(df.shape[0])
                print()
            insert_statement = insert_template.format(publish_date=df.iloc[i, 5],
                                                      title=clean_for_sql(df.iloc[i, 1]),
                                                      doi=clean_for_sql(df.iloc[i, 0]),
                                                      abstract=clean_for_sql(df.iloc[i, 2]),
                                                      server=clean_for_sql(df.iloc[i, 3]),
                                                      category=clean_for_sql(df.iloc[i, 4]))
            print_verbose(insert_statement)
            cursor.execute(insert_statement)
        except pymysql.err.ProgrammingError as err:
            print('SQL syntax error')
            print(err)
            print('The command:')
            print(insert_statement)
    cursor.close()
    connection.commit()


"""
Give a cursor for the sqlite3 db
return -1 if there are no duplicate titles
return a tuple of unique titles if there are duplicates
"""


def check_for_duplicate_titles(cursor):
    statement = """ SELECT COUNT(*) FROM articles"""
    cursor.execute(statement)
    res = cursor.fetchone()
    num_rows = int(res[0])
    print_verbose('Number of Rows: ' + str(num_rows))
    statement = """SELECT DISTINCT title FROM articles"""
    cursor.execute(statement)
    unique_titles = cursor.fetchall()
    print_verbose('Number of Unique Titles: ' + str(len(unique_titles)))

    # if the number of unique titles and the number of total rows there are no duplicated, return
    if len(unique_titles) == num_rows:
        return -1
    else:
        return unique_titles


"""
Give a cursor and a connection to the sqlite3 db
Remove all duplicate articles
Duplicates are defined as having the same publish date and title
"""


def remove_duplicate_articles(connection, cursor):
    unique_titles = check_for_duplicate_titles(cursor)
    if unique_titles == -1:
        return

    # for each title, check if there are duplicates, remove the older duplicates
    statement = """SELECT article_id, publish_date FROM articles WHERE title='{title}'"""
    for title in unique_titles:
        clean_title = title[0].replace("'", "''")
        sql_statement = statement.format(title=clean_title)
        cursor.execute(sql_statement)
        res = cursor.fetchall()
        if len(res) > 1:
            print_verbose('Duplicate: ' + clean_title + ' ' + str(len(res)))
            # get all the dates
            dates = [x[1] for x in res]
            ids = [x[0] for x in res]
            # check if the dates are unique
            if len(dates) == len(set(dates)):
                return
            # make a map of dates and ids all sharing that date
            date_id_map = {d: [ids[i] for i in range(len(ids)) if dates[i] == d] for d in set(dates)}
            print_verbose('\t' + str(date_id_map))
            # for each date if there are multiple ids delete the later ones
            delete_statement = """DELETE FROM articles WHERE article_id='{id}'"""
            for date in date_id_map.keys():
                if len(date_id_map[date]) == 1:
                    continue
                for i in range(1, len(date_id_map[date])):
                    formatted_delete = delete_statement.format(id=date_id_map[date][i])
                    print_verbose('\t' + formatted_delete)
                    cursor.execute(formatted_delete)
    # commit all the transactions
    connection.commit()


if __name__ == "__main__":
    start = time.time()
    today = date.today()
    todays_date = today.strftime("%Y-%m-%d")

    con = open_db()
    curs = con.cursor()

    # this deletes what is already in the DB and creates an empty one
    create_db(con)

    # d = load_biorxiv_data('Data/processed_api_collected_article_data_{}.tsv'.format(todays_date))
    d = load_biorxiv_data('Data/api_collected_data_2021-10-15.tsv')

    curs = con.cursor()
    print('Removing Duplicates')
    # remove_duplicate_articles(con, curs)

    print('Adding')
    # VERBOSE=True
    add_articles(con, d)
    print('Time Spent Adding: ' + str(time.time() - start))

    print(curs.execute("SELECT article_id FROM articles"))
    result = curs.fetchall()
    print('Total Articles: ' + str(len(result)))

    con.close()
