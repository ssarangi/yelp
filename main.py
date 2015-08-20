__author__ = 'sarangis'

from mongodb import *
from parallels import *
from bokeh.plotting import figure
from collections import OrderedDict
from bokeh.charts import Bar, output_file, show
import numpy as np
from multiprocessing import freeze_support

def test_database():
    mongo_helper = MongoDBHelper('yelp')
    five_star_reviews = mongo_helper.reviews.query().filter(stars = 5).count().execute().get()

    four_star_reviews = mongo_helper.reviews.query().filter(stars = 4).count().execute().get()

    three_star_reviews = mongo_helper.reviews.query().filter(stars = 3).count().execute().get()

    two_star_reviews = mongo_helper.reviews.query().filter(stars = 2).count().execute().get()

    one_star_reviews = mongo_helper.reviews.query().filter(stars = 1).count().execute().get()

    xyvalues = OrderedDict()
    xyvalues["xy"] = [one_star_reviews, two_star_reviews, three_star_reviews, four_star_reviews, five_star_reviews]
    x_labels = ["1 star", "2 star", "3 star", "4 star", "5 star"]

    bar = Bar(xyvalues, x_labels, title="Review Stars", xlabel="Stars", ylabel="Review")

    output_file("reviews.html")

    show(bar)

import nltk
from nltk.corpus import stopwords
stopwords_list = stopwords.words('english')

class CleanStopwords(ParallelBase):
    def __init__(self, stopword_list):
        ParallelBase.__init__(self)
        self.stopwords = stopword_list
        self.result = []

    def runPar(self, word):
        if not (word in self.stopwords):
            return word

    def runComplete(self, arg):
        self.result = [word for word in arg if word != None]

def create_txt(df):
    col = df["text"]
    txt = ""

    for line in col:
        txt += " " + line

    return txt

def create_txt_from_cursor(cursor):
    txt = ""
    for doc in cursor:
        txt += " " + doc["text"]

    return txt

def remove_punct(txt):
    tokens = nltk.wordpunct_tokenize(txt)
    text = nltk.Text(tokens)
    words = [w.lower() for w in text if w.isalpha()]
    return words

def clean_stopwords():
    # freeze_support()
    # worker([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15])
    mongo_helper = MongoDBHelper('yelp')
    five_star_review_txt_df = mongo_helper.reviews.query().filter(stars = 5).projection(text=1, _id=0).limit(18000).execute().get_cursor() #.dataframe()
    five_star_txt = create_txt_from_cursor(five_star_review_txt_df)
    five_star_txt.encode("utf-8")
    print("Tokenizing the text from 5 star reviews")
    five_star_txt_tokenized = remove_punct(five_star_txt)
    # five_star_txt_tokenized = remove_punct("The cow was walking on the road. The cars were stopping due to the cow being on the road. However, no one was doing anything")
    print("Starting to clean stopwords")
    cleanStopwords = CleanStopwords(stopwords_list)
    cleanStopwords.map(five_star_txt_tokenized)

    cleaned_txt = [word.encode("utf-8") for word in cleanStopwords.result]

    return cleaned_txt

def compute_ngrams(txt):
    from sklearn.feature_extraction.text import CountVectorizer
    word_vectorizer = CountVectorizer(ngram_range=(1,3), stop_words='english', analyzer='word')
    trainset = word_vectorizer.fit_transform(txt)

if __name__ == "__main__":
    # mongo_helper = MongoDBHelper('yelp')
    # # my_query = mongo_helper.reviews.query().filter(Q(stars = 5) | Q(stars=4)).execute().dataframe()
    # # four_star_reviews = mongo_helper.reviews.query().filter(stars = 4).count().execute().get()
    # # print(my_query)
    # reviews_txt_df = mongo_helper.reviews.query().filter(stars = 4).projection(text=1, _id=0).execute().dataframe()
    # col = reviews_txt_df["text"]
    #
    # txt = ""
    # for line in col:
    #     txt += " " + line
    #
    # from wordcloud import WordCloud
    # word_cloud = WordCloud().generate(txt)
    # word_cloud.to_file("wordcloud.png")
    # plot = figure(width=1000, height=1000)
    # pix = np.array(word_cloud.to_image())
    # plot.image_rgba(image=[pix], x=[0], y=[0], dw=[10], dh=[10])
    # output_file("wordcloud.html")
    #
    # show(plot)
    # main()

    # map_reduce = MapReduce()
    # map_reduce.map(worker, [1,2,3,4,5,6,7,8])
    clean_stopwords()