__author__ = 'sarangis'

import nltk
from nltk.corpus import stopwords
stopwords_list = stopwords.words('english')
from mongodb import *
from parallels import *

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

class ParCountVectorizer(ParallelBase):
    def __init__(self):
        self.trigram_freq = {}

    def _increment_count_in_dict(self, dict, key):
        if (key in dict):
            dict[key] += 1
        else:
            dict[key] = 1

    # arg is a list of words or just a string
    def runPar(self, arg):
        # We expect the arg to be a string
        trigram_freq = {}
        bigram_freq = {}
        unigram_freq = {}

        word_list = remove_punct(arg)
        # Remove the stopwords
        word_list = [word for word in word_list if word not in stopwords_list]

        # Now this is a list of words
        wl1 = word_list[1:]
        wl2 = word_list[2:]

        unigram = word_list
        zipped_bigram = zip(word_list, wl1)
        zipped_trigram = zip(word_list, wl1, wl2)

        for item in unigram:
            self._increment_count_in_dict(unigram_freq, item)

        for item in zipped_bigram:
            self._increment_count_in_dict(bigram_freq, item)

        for item in zipped_trigram:
            self._increment_count_in_dict(trigram_freq, item)

        return (unigram_freq, bigram_freq, trigram_freq)

    def runComplete(self, arg):
        self.trigram_freq = {}
        self.bigram_freq = {}
        self.unigram_freq = {}

        for a in arg:
            unigram_freq = a[0]
            bigram_freq = a[1]
            trigram_freq = a[2]

            for k,v in trigram_freq.items():
                self._increment_count_in_dict(self.trigram_freq, k)

            for k,v in bigram_freq.items():
                self._increment_count_in_dict(self.bigram_freq, k)

            for k,v in unigram_freq.items():
                self._increment_count_in_dict(self.unigram_freq, k)