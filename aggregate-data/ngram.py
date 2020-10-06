import pandas as pd
import re
import time
import datetime
from os import listdir
import pymysql
from databaseaccess import DataBaseAccess
from databaseaccess import unix_to_unix_year_month, date_to_unix_time
from buildSqlTable import clean_for_sql

import datetime

VERBOSE = True


def print_verbose(string):
    global VERBOSE
    if VERBOSE:
        print(string)


"""
I hold this function's function to be self evident
"""


def int_to_string_month(month):
    months_map = {
        1: 'January',
        2: 'February',
        3: 'March',
        4: 'April',
        5: 'May',
        6: 'June',
        7: 'July',
        8: 'August',
        9: 'September',
        10: 'October',
        11: 'November',
        12: 'December',
    }
    return months_map[month]


"""
I hold this function's function to be self evident
"""


def string_to_int_month(month):
    months_map = {
        'January': 1,
        'February': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'August': 8,
        'September': 9,
        'October': 10,
        'November': 11,
        'December': 12,
    }
    return months_map[month]


"""
Give an db connection and a word of a number
Create a tables called <the given number>_gram 
"""


def create_n_gram_table(db, n):
    create_template = """CREATE TABLE IF NOT EXISTS {ngram}_gram (
        gram_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        gram TEXT NOT NULL,
        count INT NOT NULL,
        unix_date INT NOT NULL,
        year INT NOT NULL,
        month TEXT NOT NULL);
        """
    # drop_temp = "DROP TABLE {ngram}_gram;"
    # db.cursor.execute(drop_temp.format(ngram=n))
    create_statement = create_template.format(ngram=n)
    print_verbose(create_statement)
    db.cursor.execute(create_statement)


"""
Given a db object
Create some dumb data for the db
"""


def add_dumby_one_grams(db):
    template = """
    INSERT INTO one_gram (unix_date, gram, count, year, month) VALUES ({unix_date},'{gram}',{count},{year},'{month}');
    """
    unix_dates = [1587261301, 1555638901, 1524102901, 1587261301]
    grams = ['banana', 'cheese', 'cream', 'tuna']
    years = [2019, 2018, 2017, 2019]
    counts = [5, 3, 4, 1]
    months = ['March', 'March', 'March', 'March']
    for i in range(len(unix_dates)):
        statement = template.format(unix_date=unix_dates[i], gram=grams[i], year=years[i], month=months[i],
                                    count=counts[i])
        print_verbose(statement)
        db.cursor.execute(statement)
    db.connection.commit()


"""
Give a db
return the unix date (int), year (int) and month (int) of the most recent item in one_gram
"""


def most_recent_date(db):
    select_statement = """SELECT unix_date, year, month FROM one_gram ORDER BY unix_date DESC;"""
    db.cursor.execute(select_statement)
    res = db.cursor.fetchone()
    if res is None:
        return 0, 1970, 1
    unix = res[0]
    year = res[1]
    month_string = res[2]
    return unix, year, string_to_int_month(month_string)


"""
Given a db object and a year (int) and month (int)
Delete all rows from all tables from that month and that year
"""


def delete_grams_with_date(db, year, month):
    print_verbose('Deleting Grams from year: ' + str(year) + ' and month: ' + str(month))
    template = """
    DELETE FROM {ngram}_gram WHERE year={year} and month='{month}';
    """
    for n in ['one', 'two', 'three']:
        statement = template.format(ngram=n, year=year, month=int_to_string_month(month))
        print_verbose('\t' + statement)
        db.cursor.execute(statement)
    db.connection.commit()
    db.cursor.execute("SELECT * FROM one_gram")
    print_verbose(db.cursor.fetchall())


"""
Given:
db - DataBaseAccess object
unix_time_start - unix time stamp to start collecting data from
unix_time_end - unix time stamp to collect data until
"""


def get_abstracts(db, unix_time_start, unix_time_end):
    template = """
    SELECT abstract FROM articles WHERE publish_date >= {start_date} AND publish_date < {end_date};
    """
    statement = template.format(start_date=unix_time_start, end_date=unix_time_end)
    db.cursor.execute(statement)
    abs = db.cursor.fetchall()
    # convert the results into a list
    abs_list = [x[0] for x in abs]
    print_verbose('\tNumber of abstracts: ' + str(len(abs_list)))
    return abs_list


"""
Given a year (int) and month (int, 1 based)
Return the unix time start of the first day of that month and the first day of the next month
"""


def get_unix_start_and_end(year, month):
    start = int_to_string_month(month) + ' 1, ' + str(year)
    end_month = month + 1
    end_year = year
    if month == 12:
        end_month = 1
        end_year += 1
    end = int_to_string_month(end_month) + ' 1, ' + str(end_year)
    start_unix = date_to_unix_time(start)
    end_unix = date_to_unix_time(end)
    return start_unix, end_unix


"""
Given some text (string) and n (int) the size of the n-gram
gram_dict - optional param of a dictionary of gram: count to start from
Return a pandas DataFrame of the n-grams and their respective counts
"""


def get_n_grams(text, n, gram_dict={}):
    # if a special character is being used as punctuation (not in a name) add a space
    text = re.sub('(: )', ' \\g<1>', text)
    text = re.sub('(- )', ' \\g<1>', text)
    text = re.sub('(, )', ' \\g<1>', text)
    text = re.sub('(\\. )', ' \\g<1>', text)
    text = re.sub('(- )', ' \\g<1>', text)
    text = re.sub('(\\? )', ' \\g<1>', text)
    text = re.sub('(; )', ' \\g<1>', text)
    text = re.sub('(! )', ' \\g<1>', text)
    # remove paranthesis arounda  single word
    text = re.sub(' \\(([^ ])\\) ', ' \\g<1> ', text)
    # remove leading and trailing parenthesis
    text = re.sub(' \\(', ' ', text)
    text = re.sub('\\) ', ' ', text)
    text_list = text.split(' ')

    # create the n-grams
    done = False
    # gram_dict = {}
    for i in range(len(text_list)):
        gram = ''
        skip = False
        for j in range(n):
            if i + j >= len(text_list):
                done = True
                break
            # check if the current item is punctuation, if so skip this gram
            if text_list[i + j] in ['.', ',', '?', ';', '!', ':', '-']:
                skip = True
                break
            gram += text_list[i + j] + ' '
        if not done and not skip:
            # remove trailing space
            gram = gram[:-1]
            # if gram has already been made
            if gram in gram_dict:
                # increment count
                gram_dict[gram] += 1
            else:
                # else create new entry
                gram_dict[gram] = 1
    gram_df = pd.DataFrame({'gram': list(gram_dict.keys()), 'count': list(gram_dict.values())})
    return gram_df, gram_dict


def add_df_to_n_gram_table(db, table, df):
    template = "INSERT INTO {table} (gram, count, unix_date, year, month) VALUES (%s, %s, %s, %s, %s);"
    sql_statements = template.format(table=table)
    print_verbose('\tSize of df: ' + str(df.shape[0]))
    start = time.time()
    values = df.values.tolist()
    db.cursor.executemany(sql_statements, values)
    db.connection.commit()
    db.cursor.execute("SELECT gram FROM {table}".format(table=table))
    print_verbose('\tNumber of things in table ' + table + ' ' + str(len(db.cursor.fetchall())))
    print_verbose('\tTable Time: ' + str(time.time() - start))


"""
Given:
db - DataBaseAccess object
abstracts - list of abstracts (string)
unix - unix time stramp of the year-month-01 the abstracts were published during
year - year abstracts were published (int)
month - month abstracts were published in (int 1 based)
Generate lists of 1 - 3 grams and their counts for all abstracts
Adds grams and counts to DB 
"""


def make_n_grams(db, abstracts, unix_date, year, month):
    grams = ['one', 'two', 'three']
    grams_num = [1, 2, 3]
    for i in range(len(grams)):
        gram_dict = {}
        for ab in abstracts:
            df, gram_dict = get_n_grams(ab, grams_num[i], gram_dict)
        df['unix_date'] = unix_date
        df['year'] = year
        df['month'] = int_to_string_month(month)
        add_df_to_n_gram_table(db, grams[i] + '_gram', df)


def do_update():
    db = DataBaseAccess()
    # create the tables if they does not exist
    for n in ['one', 'two', 'three']:
        create_n_gram_table(db, n)
    # get the most recent date
    # add_dumby_one_grams(db)
    unix, year, month = most_recent_date(db)
    # delete all entries that came from the same year and month
    delete_grams_with_date(db, year, month)

    # count from most recent year / month to now
    now = datetime.datetime.now()
    first = True
    for y in range(year, now.year + 1):
        print_verbose('Doing Year: ' + str(y))
        end_month = 13
        if y == now.year:
            end_month = now.month + 1

        start_month = 1
        # if only scrapping the first year
        if first and y == now.year:
            first = False
            start_month = month
        else:
            first = False
        for m in range(start_month, end_month):
            print_verbose('\tMonth ' + str(m))
            unix_start, unix_end = get_unix_start_and_end(y, m)
            abstracts = get_abstracts(db, unix_start, unix_end)
            if len(abstracts) == 0:
                continue
            make_n_grams(db, abstracts, unix_start, y, m)


do_update()
