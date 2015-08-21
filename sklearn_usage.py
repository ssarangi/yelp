__author__ = 'sarangis'

def compute_sklearn_ngrams(txt):
    from sklearn.feature_extraction.text import CountVectorizer
    word_vectorizer = CountVectorizer(ngram_range=(1,3), stop_words='english', analyzer='word')
    trainset = word_vectorizer.fit(txt)
    return word_vectorizer

