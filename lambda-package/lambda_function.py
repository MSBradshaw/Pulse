import sys
import logging
# import is formated this way to work in AWS lambda
from package import pymysql
from databaseaccess import DataBaseAccess

db = DataBaseAccess()

logger = logging.getLogger()
logger.setLevel(logging.INFO)


"""
Given a dictionary of requested information
Get the data for all search terms combined into one list 
Return the data in a dictionary with one item
"""


def handler(event, context):
    print(event)
    search_results = []
    for term in event['terms']:
        result = db.get_word_like(term, event['from_date'], event['to_date'])
        search_results.append(result)
    return {'data': search_results}



