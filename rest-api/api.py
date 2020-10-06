import flask
from flask import request
from databaseaccess import DataBaseAccess
from flask_cors import CORS, cross_origin
import time

app = flask.Flask(__name__)

cors = CORS(app)
app.config["DEBUG"] = True
app.config['CORS_HEADERS'] = 'Content-Type'

"""
Simple Custom Cache
Constructor takes 1 parameter, the max size
If the max size is reached the first 5 things in the cache will be deleted to free up space
"""


class MaxSizeCache:
    def __init__(self, size):
        self.cache = {}
        self.size = size
        self.birth_time = time.time()

    def in_cache(self, key):
        self.check_and_clear()
        return key in self.cache.keys()

    def add_to_cache(self, key, value):
        # if the max size have been reached delete the first 5 keys
        self.check_and_clear()
        self.manage_size()
        self.cache[key] = value

    def get(self, key):
        return self.cache[key]

    def check_and_clear(self):
        # check if the cache is older than 3 hours
        if self.birth_time + 10800 < time.time():
            print('Resenting Cache')
            self.cache = {}
            self.birth_time = time.time()

    def manage_size(self):
        if len(self.cache) == self.size:
            print('Removing Some Cache Items')
            keys = list(self.cache.keys())
            for i in range(5):
                del self.cache[keys[i]]

cache = MaxSizeCache(100)


@app.route('/', methods=['POST'])
def home():
    if request.method == 'POST':
        req_json = request.get_json()
        print(req_json)
        db = DataBaseAccess(False)
        search_results = []
        start = time.time()
        for t in req_json['terms']:
            # create a key for the cache
            key = t + req_json['from_date'] + req_json['to_date']
            # check if the key is in the cache already
            if cache.in_cache(key):
                print('In Cache')
                search_results.append(cache.get(key))
            else:
                print('Not In Cache')
                results = db.get_word_like(t, req_json['from_date'], req_json['to_date'])
                search_results.append(results)
                cache.add_to_cache(key, results)

        db.connection.close()
        print('---------')
        print(len(search_results))
        print('Run time: ' + str(time.time() - start))
        print(search_results)
        return {'data': search_results}


app.run()
