__author__ = 'sarangis'

import pandas as pd
from pymongo import MongoClient

class QueryBase:
    _query_dict = {}
    def __init__(self):
        self._query_dict = {}

    def _is_op(self, param):
        ops = ["lt"
               "gt",
               "lte",
               "gte",
               "ne",
               "exists",
               "regex",
               "in",
               "all"]

        if (any(param in i for i in ops)):
            return True

        return False

    def _parse_key(self, key):
        params = key.split("__")
        return params

    def _parse_query_dict(self, kwargs):
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

        return self._query_dict

# This is basically a QueryNode
class Q(QueryBase):
    def __init__(self, **kwargs):
        QueryBase.__init__(self)
        self._query_dict = self._parse_query_dict(kwargs)

    def __or__(self, other):
        self._query_dict = { "$or": [self._query_dict, other._query_dict] }
        return self

    def __and__(self, other):
        self._query_dict = { "$and": [self._query_dict, other._query_dict] }
        return self

class Query(QueryBase):
    def __init__(self, db, collection):
        QueryBase.__init__(self)
        self._cursor = None
        self._count = False
        self._projection = {}
        self._db = db
        self._collection = collection
        self._value = None
        self._limit = 0

    def filter(self, *args, **kwargs):
        if (len(args) == 0):
            self._parse_query_dict(kwargs)
        else:
            self._query_dict = args[0]._query_dict
        return self

    def projection(self, **kwargs):
        self._projection = kwargs
        return self

    def count(self):
        self._count = True
        return self

    def limit(self, limit_val):
        self._limit = limit_val
        return self

    def execute(self):
        if (self._count):
            self._count = self._db[self._collection].find(self._query_dict).count()
            self._value = self._count
        else:
            if len(self._projection) > 0:
                if (self._limit > 0):
                    self._cursor = self._db[self._collection].find(self._query_dict, self._projection).limit(self._limit)
                else:
                    self._cursor = self._db[self._collection].find(self._query_dict, self._projection)
                self._value = self._cursor
            else:
                if (self._limit > 0):
                    self._cursor = self._db[self._collection].find(self._query_dict).limit(self._limit)
                else:
                    self._cursor = self._db[self._collection].find(self._query_dict)
                self._value = self._cursor

        return self

    def get(self):
        return self._value

    def dataframe(self):
        if (self._count == True):
            raise("Dataframe cannot be used with Count method")
        # Expand the cursor and construct the dataframe
        df = pd.DataFrame(list(self._cursor))

        self._value = df
        return df

    def __str__(self):
        return str(self._query_dict)

class Collection:
    def __init__(self, db_obj, name, parent_name=""):
        self._collection_name = name
        self._db = db_obj
        self._cursor = None
        self._count = 0

    def query(self):
        return Query(self._db, self._collection_name)

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