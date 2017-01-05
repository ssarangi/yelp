import os
import time
import json

from pymongo import MongoClient

from settings import Settings
from utils import ProgressBar

def print_header(txt):
    print("-" * 50)
    print("\t%s" % txt)
    print("-" * 50)


class MongoDBConverter:
    def __init__(self):
        self.db = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE]
        self.progess_bar = ProgressBar()

    def create_review_db(self):
        print_header("Creating Reviews")
        done = 0
        dataset_file = Settings.REVIEW_DATASET_FILE
        business_collection = self.db[Settings.BUSINESS_COLLECTION]

        # Find all the businesses and their reviews and add the review_id to a dict.
        review_id_hashes = set()

        businesses = business_collection.find()
        for business in businesses:
            if 'reviews' in business:
                for review in business['reviews']:
                    review_id_hashes.add(review['review_id'])

        self.progess_bar.start()
        with open(dataset_file, 'r') as dataset:
            count = sum(1 for _ in dataset)

        with open(dataset_file, 'r') as dataset:
            next(dataset)
            for line in dataset:
                try:
                    data = json.loads(line, encoding='utf-8')
                except ValueError:
                    print('Oops!')

                # Insert into DB
                if data["type"] == "review":
                    business_id = data['business_id']
                    business = business_collection.find_one({'business_id': business_id})

                    assert(business is not None)

                    add_review = True
                    if data['review_id'] in review_id_hashes:
                        add_review = False

                    business['reviews'] = business.get('reviews', [])
                    if add_review:
                        business['reviews'].append(data)
                        business_collection.update_one({'business_id': business_id}, {"$set": business}, upsert=True)

                done += 1
                self.progess_bar.print_progress(done, count)

    def add_business_data_collection(self):
        print_header("Adding Business Data")

        dataset_file = Settings.BUSINESS_DATASET_FILE

        add_businesses = True

        if Settings.BUSINESS_COLLECTION in self.db.collection_names():
            business_collection = self.db[Settings.BUSINESS_COLLECTION]
            if business_collection.count() > 0:
                add_businesses = False
                print("Data already present.... Skipping")

        if add_businesses:
            self.progess_bar.start()
            with open(dataset_file, 'r') as dataset:
                count = sum(1 for _ in dataset)

            business_collection = self.db[Settings.BUSINESS_COLLECTION]
            with open(dataset_file, 'r') as dataset:
                done = 0
                for line in dataset:
                    try:
                        data = json.loads(line, encoding='utf-8')
                    except ValueError:
                        print("Error in Business json file")

                    # Insert into DB
                    assert(data['type'] == 'business')
                    business_collection.insert(data)

                    done += 1
                    self.progess_bar.print_progress(done, count, prefix='Progress:', suffix='Complete')

            business_collection.create_index('business_id')

    def add_user_data_collection(self):
        print_header("Adding User Data")
        dataset_file = Settings.USER_DATASET_FILE

        add_users = True

        if Settings.USER_COLLECTION in self.db.collection_names():
            user_collection = self.db[Settings.USER_COLLECTION]
            if user_collection.count() > 0:
                add_users = False
                print("Data already present.... Skipping")

        if add_users:
            self.progess_bar.start()
            with open(dataset_file, 'r') as dataset:
                count = sum(1 for _ in dataset)

            user_collection = self.db[Settings.USER_COLLECTION]
            with open(dataset_file, 'r') as dataset:
                done = 0
                for line in dataset:
                    try:
                        data = json.loads(line, encoding='utf-8')
                    except ValueError:
                        print("Error in Business json file")

                    # Insert into DB
                    assert(data['type'] == 'user')
                    user_collection.insert(data)

                    done += 1
                    self.progess_bar.print_progress(done, count, prefix='Progress:', suffix='Complete')

            user_collection.create_index('user_id')


def main():
    mongo = MongoDBConverter()
    mongo.add_business_data_collection()
    mongo.add_user_data_collection()
    mongo.create_review_db()

if __name__ == "__main__":
    main()
