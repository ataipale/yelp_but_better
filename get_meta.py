"""
Script for generating restaurant metadata and inserting it into a MongoDB

notes: make sure mongod running. use `sudo mongod` in terminal
"""
# for Ubuntu...
import search as yelp
# for local machine...
# from yelp import search as yelp
from pymongo import MongoClient
from pymongo import errors
import time
import argparse
import os
import pickle

"""
Insert user's Yelp API key information here
"""

KEY = os.environ.get('CKEY_YELP')
SECRET_KEY = os.environ.get('CSECRET_YELP')
TOKEN = os.environ.get('TOKEN_YELP')
SECRET_TOKEN = os.environ.get('TOKEN_SECRET_YELP')




class GetMeta(object):
    """ This class uses the Yelp search API to scrape business metadata and
    insert it into a MongoDB """

    def __init__(self, database, table_name,
                 key, secret_key, token, secret_token, params):
        client = MongoClient()
        self.database = client[database]
        self.table = self.database[table_name]
        self.key = key
        self.secret_key = secret_key
        self.token = token
        self.secret_token = secret_token
        self.params = params

    def make_request(self):
        """Using the search terms and API keys,
           connect and get from Yelp API"""

        return yelp.request(self.params, self.key, self.secret_key,
                            self.token, self.secret_token)

    def insert_business(self, rest):
        """
        INPUT:
        param rest -- Dictionary object containing meta-data to be
                      inserted in Mongo

        OUTPUT: None

        Inserts dictionary into MongoDB
        """

        if not self.table.find_one({"id": rest['id']}):
            # Make sure all the values are properly encoded
            for field, val in rest.iteritems():
                if type(val) == str:
                    rest[field] = val.encode('utf-8')

            try:
                print "Inserting restaurant " + rest['name']
                self.table.insert(rest)
            except errors.DuplicateKeyError:
                print "Duplicates"
        else:
            print "In collection already"

    def run(self):

        try:
            response = self.make_request()
            total_num = response['total']
            print 'Total number of entries for the query', total_num
            with open("neighborhoods_checked.txt", "a") as f:
                if total_num > 1000: 
                    f.write(self.params['location'])
                    f.write('m')
                else:
                    f.write(self.params['location'])
                    f.write('x')

            while self.params['offset'] < total_num:
                response = self.make_request()
                try:
                    for business in response['businesses']:
                        self.insert_business(business)
                except:
                    print 'TOO MANY RESTAURANTS IN CATEGORY:'
                    print self.params['category_filter']
                    print response
                self.params['offset'] += 20
                time.sleep(1)
        except:
            print response, self.params['category_filter']


def get_restaurant_metadata(city, db_name, coll_name):
    """
    Loads restaurant data for 'city' into the 'coll_name' collection
    of the 'db_name' database

    INPUT:  city -- city name in Yelp API format (string)
            db_name -- MongoDB name (string)
            coll_name -- MongoDB collection name (string)

    OUTPUT: None
    """

    PARAMS = {'location': "Seattle",
              'term': get_food_category(),
              'category_filter': '',
              'limit': 20,
              'offset': 0}

    yelp_meta = GetMeta(db_name, coll_name,
                        KEY, SECRET_KEY, TOKEN,
                        SECRET_TOKEN, PARAMS)
    yelp_meta.run()

def get_neighborhood():
    '''
    Yelp API has a 1000 request limit, so I am breaking up the requests by neighborhood.
    This dict will keep track of the restaurants to search. 
    '''
    with open("neighborhoods.list", "rw") as f:
        neigh_list = f.readlines()
        neigh_query = neigh_list.pop()
    with open("neighborhoods.list", "w") as f:
        rewrite = ''.join(i for i in neigh_list)
        f.write(rewrite)
    return neigh_query

def get_food_category():
    '''
    Yelp API has a 1000 request limit, so I am breaking up the requests by neighborhood.
    This dict will keep track of the restaurants to search. 
    '''
    with open("food_style.list", "rw") as f:
        food_list = f.readlines()
        food_query = food_list.pop()
    with open("food_style.list", "w") as f:
        rewrite = ''.join(i for i in food_list)
        f.write(rewrite)
    return food_query


if __name__ == '__main__':

    while True:
        neigh_city = get_neighborhood().strip() + " Seattle"
        get_restaurant_metadata("Seattle", 'restaurants', 'seattle_only_meta')
