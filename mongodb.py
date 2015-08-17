__author__ = 'sarangis'

import pandas as pd
from pymongo import MongoClient

class Query:
    def __init__(self):
        self._query_dict = {}
        self._count = False

    def _is_op(self, param):
        ops = ["lt"
               "gt",
               "lte",
               "gte",
               "ne"]

        if (any(param in i for i in ops)):
            return True

        return False

    def _parse_key(self, key):
        params = key.split("__")
        return params


    def filter(self, **kwargs):
        # Iterate over the kw args and create the search dictionary
        for k,v in kwargs.items():
            # Parse the key and figure out if there is an operation
            keys = self._parse_key(k)
            last_param = keys[len(keys) - 1]

            # See how many params we need to add
            if (self._is_op(last_param)):
                keys = keys[:-1]

            key_str = ".".join(keys)

            if (self._is_op(last_param)):
               last_param = "$" + last_param
               self._query_dict.update({key_str: {last_param: v}})
            else:
                self._query_dict.update({key_str: v})

        return self

    def count(self):
        self._count = True
        return self

    def is_count(self):
        return self._count

    def get_query(self):
        return self._query_dict

    def __str__(self):
        return str(self.query_dict)

class Collection:
    def __init__(self, db_obj, name, parent_name=""):
        self._collection_name = name
        self.db = db_obj
        self._cursor = None
        self._count = 0

        find_one = self.db[self._collection_name].find_one()

        if (find_one != None):
            collection_list = find_one.keys()
            for collection in collection_list:
                print("Creating Collection: %s" % parent_name + "." + collection)
                setattr(self, collection, Collection(self.db, collection, self._collection_name))

    def _query(self, query_dict={}):
        cursor = None
        if bool(query_dict):
            cursor = self.db[self._collection_name].find()
        else:
            cursor = self.db[self._collection_name].find(query_dict)

        return cursor

    def dataframe(self, no_id=True):
        # Expand the cursor and construct the dataframe
        df = pd.DataFrame(list(self._cursor))

        if (no_id):
            del df['_id']

        return df

    def count(self):
        return self._count

    def query(self):
        return Query()

    def execute_query(self, query):
        if (query.is_count()):
            self._count = self.db[self._collection_name].find(query.get_query()).count()
        else:
            self._cursor = self.db[self._collection_name].find(query.get_query())

        return self

class MongoDBHelper:
    def __init__(self, db, host='localhost', port=27017, username=None, password=None, no_id=True):
        # Connect to MongoDB
        self.db = self._connect_mongo(host=host, port=port, username=username, password=password, db=db)

        # initialize all the members with collection classes
        collection_list = self.db.collection_names(include_system_collections=False)

        for collection in collection_list:
            print("Creating Collection: %s" % collection)
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
    query = mongo_helper.users.query().filter(average_stars__gt = 2).filter(votes__funny__gt = 2)
    print(mongo_helper.users.execute_query(query).dataframe())
