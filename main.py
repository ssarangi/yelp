__author__ = 'sarangis'

from mongodb import *
from parallels import *
from bokeh.plotting import figure
from collections import OrderedDict
from bokeh.charts import Bar, output_file, show
from parallel_nlp import *
from sklearn_usage import *
import timeit

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

def use_my_count_vectorizer():
    # str = "Well, the canonical approach in Python is to not check the type at all (unless you're debugging)."
    # str_tokenized = remove_punct(str)

    mongo_helper = MongoDBHelper('yelp')

    reviews = []
    five_star_review_txt_cursor = mongo_helper.reviews.query().filter(stars = 5).projection(text=1, _id=0).limit(18000).execute().get_cursor() #.dataframe()

    for doc in five_star_review_txt_cursor:
        reviews.append(doc["text"])

    par_count_vec = ParCountVectorizer()
    par_count_vec.map(reviews)

    #for trigram, freq in par_count_vec.trigram_freq.items():
    #    print("%s:%d" % ((trigram[0] + " " + trigram[1] + " " + trigram[2]), freq))

    import operator
    sorted_trigrams = sorted(par_count_vec.trigram_freq.items(), key=operator.itemgetter(1))
    top_200 = sorted_trigrams[-200:]
    for i in top_200:
        print("%s:%d" % ((i[0][0] + " " + i[0][1] + " " + i[0][2]), i[1]))

    # for bigram, freq in par_count_vec.bigram_freq.items():
    #     print(bigram, freq)

    # for unigram, freq in par_count_vec.unigram_freq.items():
    #     print(unigram, freq)

    from nltk.stem import WordNetLemmatizer
    wordnet_lemmatizer = WordNetLemmatizer()

    

def generate_word_cloud_from_reviews():
    # Doesn't work on Windows yet
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
    pass

def par_clean_stopwords():
    clean_stopwords()

def create_sklearn_ngrams():
    print("Reading Database")
    mongo_helper = MongoDBHelper('yelp')
    five_star_review_txt_cursor = mongo_helper.reviews.query().filter(stars = 5).projection(text=1, _id=0).limit(18000).execute().get_cursor() #.dataframe()

    for doc in five_star_review_txt_cursor:
        matrix = compute_sklearn_ngrams(doc['text'].split('\n'))
        print(matrix.get_feature_names())

    five_star_txt = create_txt_from_cursor(five_star_review_txt_cursor)
    print("Database Reading finished")
    for counter, c in enumerate(five_star_txt):
        print("%d: %s" % (counter, c))

    print(five_star_txt)
    word_vectorizer = compute_sklearn_ngrams(five_star_txt)

    print(type(word_vectorizer.vocabulary))
    for k, v in word_vectorizer.vocabulary:
        print(k, v)

if __name__ == "__main__":
    use_my_count_vectorizer()



