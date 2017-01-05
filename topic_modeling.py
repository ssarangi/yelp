from pymongo import MongoClient

from settings import Settings
from utils import ProgressBar

import gensim
import spacy
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
from nltk.corpus import stopwords
from itertools import chain

from pprint import pprint

class TopicModeler:
    def __init__(self):
        self.db = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE]
        self.progess_bar = ProgressBar()

    def tokenize_text(self, nlp, txt):
        doc = nlp(txt)
        return doc

    def generate_topics(self, reviews):
        nlp = spacy.load('en')
        STOPLIST = set(stopwords.words('english') + ["n't", "'s", "'m", "ca", ".", "\n\n", ",", ";", " ", "$", "*", "&", '?', '!', '(', ')', ':', '%'] + list(ENGLISH_STOP_WORDS))
        all_tokens = []
        for review in reviews:
            tokens = self.tokenize_text(nlp, review['text'])
            stopped_tokens = [i for i in tokens if i.orth_.lower() not in STOPLIST]
            for i in stopped_tokens:
                print(i)
            all_tokens.append(stopped_tokens)

        texts = [[token.lemma_ for token in token_list] for token_list in all_tokens]
        dictionary = gensim.corpora.Dictionary(texts)
        corpus = [dictionary.doc2bow(text) for text in texts]
        ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=5, id2word=dictionary, passes=100)

        lda_corpus = ldamodel[corpus]
        # Find the threshold, let's set the threshold to be 1/#clusters,
        # To prove that the threshold is sane, we average the sum of all probabilities:
        scores = list(chain(*[[score for topic_id, score in topic] \
                              for topic in [doc for doc in lda_corpus]]))
        threshold = sum(scores) / len(scores)
        print(threshold)

        # cluster1 = [j for i, j in zip(lda_corpus, reviews) if i[0][1] > threshold]
        # cluster2 = [j for i, j in zip(lda_corpus, reviews) if i[1][1] > threshold]
        # cluster3 = [j for i, j in zip(lda_corpus, reviews) if i[2][1] > threshold]
        #
        # print(cluster1)
        # print(cluster2)
        # print(cluster3)

        print(ldamodel.print_topics(num_topics=5, num_words=3))
        return ldamodel

    def create_topics_from_reviews(self):
        business_collection = self.db[Settings.BUSINESS_COLLECTION]

        businesses = business_collection.find()

        for business in businesses:
            if 'reviews' in business:
                self.generate_topics(business['reviews'])


if __name__ == "__main__":
    topic_modeler = TopicModeler()
    topic_modeler.create_topics_from_reviews()