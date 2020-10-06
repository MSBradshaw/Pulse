import datetime
import time
from package import pymysql


"""
Give a date (example "June 21, 2019")
Return the unix time of that date
Return None if string is empty or None
"""


def date_to_unix_time(date):
    if date is None or date == '':
        return None
    dt = datetime.datetime.strptime(date, '%B %d, %Y')
    return int(time.mktime(dt.timetuple()))


"""
Given a unix time stamp (int)
Return the the month and year of that time ("June 2019")
"""


def unxi_to_date_month(unix_time):
    date = datetime.datetime.utcfromtimestamp(unix_time).strftime('%Y-%m') + "-01"
    return date


"""
Give an unix time stamp
strip the day from the date so the date is the original year and month but always the first day of the month
Return the Year-month-first day unix time stamp
"""


def unix_to_unix_year_month(unix_time):
    date = datetime.datetime.utcfromtimestamp(unix_time).strftime('%Y-%m') + "-01"
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return int(time.mktime(dt.timetuple()))


"""
Given a dictionary of unixtime to lists
Return a dictionary in the same format but will all missing months included in dict
"""

def fill_in_missing_dates(dict):
    if len(dict) == 0:
        return dict
    # get the minimum and maximum date
    minimum = min(dict.keys())
    maximum = max(dict.keys())

    # convert to dates and then to datetime.datetime objects
    min_date = unxi_to_date_month(minimum)
    min_ts = datetime.datetime.strptime(min_date, '%Y-%m-%d')
    max_date = unxi_to_date_month(maximum)
    max_ts = datetime.datetime.strptime(max_date, '%Y-%m-%d')

    # loop over all months from the minimum to the maximum,
    current = min_ts
    all_dates_dict = {}
    while current.year < max_ts.year or current.month != max_ts.month:
        current_unix_time = date_to_unix_time(current.strftime('%B %d, %Y'))
        if current_unix_time in dict.keys():
            # print('Already Exists:')
            # print(current)
            all_dates_dict[current_unix_time] = dict[current_unix_time]
        else:
            all_dates_dict[current_unix_time] = []

        if current.month != 12:
            current = current.replace(month=current.month + 1)
        else:
            current = current.replace(year=current.year + 1)
            current = current.replace(month=1)
    return all_dates_dict


class DataBaseAccess:
    def __init__(self, verbose=True):
        info = []
        for line in open('not_important_info.txt'):
            info.append(line.strip())
        host = info[0]
        port = 3306
        user = info[1]
        password = info[2]
        db = info[3]

        self.connection = pymysql.connect(host, user=user, port=port, passwd=password, db=db)
        self.cursor = self.connection.cursor()
        self.VERBOSE = verbose
        if verbose:
            print('Verbose On\nTo turn off printouts use DataBaseAccess("path/to/thing.db",verbose=False)')

    """
    Give a string (or something else)
    print_verbose the string if the VERBOSE global is true
    """

    def print_verbose(self, string):
        if self.VERBOSE:
            print(string)

    """
    Give the results of an SQL query as a list of tuples
    Return a map of dates (Month Year, example "June 2019") and titles / urls associated with each one
    """

    def aggregate_results_by_month_like(self, results):
        # create a map of month-year to urls and titles
        time_mentions_map = {}
        for r in results:
            # get the publishing date in terms of year and month only
            month_year = unix_to_unix_year_month(r[-1])
            if month_year in time_mentions_map.keys():
                time_mentions_map[month_year].append([r[0], r[1]])
            else:
                time_mentions_map[month_year] = [r[0], r[1]]

        # add dates from those that are missing
        time_mentions_map = fill_in_missing_dates(time_mentions_map)

        # sort the map by key value
        date_to_title_url_map = {}
        for k in sorted (time_mentions_map.keys()):
            date_to_title_url_map[unxi_to_date_month(k)] = time_mentions_map[k]
        return date_to_title_url_map

    """
    Give a search term (single string with no spaces)
    Return a map of month-year to list of article title and url representing the occurrence of that term in Biorxiv
    Optional Parameters
    from_date the date from which to start searching
    to_date the date to search untill
    to_date and from_date should be formatted like "June 21, 2019"
    """

    def get_word_like(self, term, from_date=None, to_date=None):
        # self.cursor.execute("SELECT * FROM articles")
        # result = self.cursor.fetchall()
        # print('Total Articles: ' + str(len(result)))
        start = time.time()
        from_date_unix = date_to_unix_time(from_date)
        to_date_unix = date_to_unix_time(to_date)
        # if you have both dates
        if from_date is not None and from_date != '' and to_date is not None and to_date != '':
            self.print_verbose('Searching for ' + term + ' between ' + from_date + ' and ' + to_date)
            statement = """SELECT url, title, article_id, publish_date FROM articles 
            WHERE abstract LIKE '%{term}%' and publish_date BETWEEN {from_date} AND {to_date}; """
            sql_statement = statement.format(term=term, to_date=to_date_unix, from_date=from_date_unix)

        # if you have just the to date
        elif (from_date is None or from_date == '') and (to_date is not None and to_date != ''):
            self.print_verbose('Searching for ' + term + ' before ' + to_date)
            statement = """SELECT url, title, article_id, publish_date FROM articles WHERE abstract LIKE 
            '%{term}%' and publish_date < {to_date}; """
            sql_statement = statement.format(term=term, to_date=to_date_unix)

        # if you have a from date
        elif (to_date is None or to_date == '') and (from_date is not None and from_date != ''):
            self.print_verbose('Searching for ' + term + ' after ' + from_date)
            statement = """SELECT url, title, article_id, publish_date FROM articles WHERE abstract LIKE '%{term}%' 
            and publish_date > {from_date}; """
            sql_statement = statement.format(term=term, from_date=from_date_unix)

        # default you have only the search term
        else:
            self.print_verbose('Searching for ' + term)
            statement = """SELECT url, title, article_id, publish_date FROM articles WHERE abstract LIKE '%{term}%'"""
            sql_statement = statement.format(term=term)

        self.cursor.execute(sql_statement)
        res = self.cursor.fetchall()
        self.print_verbose('Number of Results: ' + str(len(res)))

        # group the results by month, if the future have alternate time metrics, year or week
        results_dict = self.aggregate_results_by_month_like(res)
        # send back a simpler version of the data, just dates and counts, no title or URL
        x = [k for k in results_dict.keys()]
        y = [len(results_dict[k]) for k in results_dict.keys()]

        simple_res = {'x': x, 'y': y, 'type': 'scatter',  'name': term}
        print('NORMAL RUN TIME: ' + str(time.time() - start))
        return simple_res


if __name__ == '__main__':
    db = DataBaseAccess(False)
    print(db.get_word_like('machine', to_date='June 21, 2016'))
    print(db.get_word_like('machine', from_date='June 21, 2016'))
    print(db.get_word_like('machine', from_date='June 21, 2016', to_date='June 21, 2019'))
    r = db.get_word_like('genomics')
    print(r)
