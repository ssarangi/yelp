class Settings:
    def __init__(self):
        pass

    BUSINESS_DATASET_FILE = '../yelp_dataset/yelp_academic_dataset_business.json'
    CHECKIN_DATASET_FILE = '../yelp_dataset/yelp_academic_dataset_checkin.json'
    REVIEW_DATASET_FILE = '../yelp_dataset/yelp_academic_dataset_review.json'
    TIP_DATASET_FILE = '../yelp_dataset/yelp_academic_dataset_tip.json'
    USER_DATASET_FILE = '../yelp_dataset/yelp_academic_dataset_user.json'

    MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
    REVIEWS_DATABASE = "Dataset_Challenge_Reviews"
    TAGS_DATABASE = "Tags"
    BUSINESS_COLLECTION = "Businesses"
    USER_COLLECTION = "Users"