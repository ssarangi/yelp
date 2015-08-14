__author__ = 'sarangis'

import pandas as pd
from pymongo import MongoClient

class Collection:
    def __init__(self, db_obj, name):
        self.name = name
        self.db = db_obj

    def _query(self, query_dict={}):
        cursor = None
        if bool(query_dict):
            cursor = self.db(self.name).find()
        else:
            cursor = self.db[self.name].find(query_dict)

        return cursor

    def to_dataframe(self, query_dict={}, no_id=True):
        cursor = self._query(query_dict)

        # Expand the cursor and construct the dataframe
        df = pd.DataFrame(list(cursor))

        if (no_id):
            del df['_id']

        return df

class MongoDBHelper:
    def __init__(self, db, host='localhost', port=27017, username=None, password=None, no_id=True):
        # Connect to MongoDB
        self.db = self._connect_mongo(host=host, port=port, username=username, password=password, db=db)

        # initialize all the members with collection classes
        collection_list = self.db.collection_names(include_system_collections=False)

        for collection in collection_list:
            setattr(self, collection, Collection(self.db, collection))

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
    print(mongo_helper.businesses.to_dataframe())