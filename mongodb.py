__author__ = 'sarangis'

import pandas as pd
from pymongo import MongoClient

class MongoDBHelper:
    def __init__(self, db, host='localhost', port=27017, username=None, password=None, no_id=True):
        # Connect to MongoDB
        self.db = self._connect_mongo(host=host, port=port, username=username, password=password, db=db)

    def _connect_mongo(self, host, port, username, password, db):
        """ A util for making a connection to mongo """
        if username and password:
            mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
            conn = MongoClient(mongo_uri)
        else:
            conn = MongoClient(host, port)

        return conn[db]

    def collections(self):
        return self.db.collection_names(include_system_collections=False)


if __name__ == "__main__":
    mongo_helper = MongoDBHelper('yelp')
    print(mongo_helper.users.to_dataframe())