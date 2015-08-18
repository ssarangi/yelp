__author__ = 'sarangis'

from mongodb import *
import matplotlib.pyplot as plt

def main():
    mongo_helper = MongoDBHelper('yelp')
    five_star_reviews = mongo_helper.reviews.query().filter(stars = 5).count().execute().get()

    four_star_reviews = mongo_helper.reviews.query().filter(stars = 4).count().execute().get()

    three_star_reviews = mongo_helper.reviews.query().filter(stars = 3).count().execute().get()

    two_star_reviews = mongo_helper.reviews.query().filter(stars = 2).count().execute().get()

    one_star_reviews = mongo_helper.reviews.query().filter(stars = 1).count().execute().get()

    y = [one_star_reviews, two_star_reviews, three_star_reviews, four_star_reviews, five_star_reviews]
    N = len(y)
    # x = ["1 star", "2 star", "3 star", "4 star", "5 star"]
    x = range(N)
    width = 1/1.5

    plt.bar(x, y, width)
    plt.xticks()

    fig = plt.figure()
    ax = plt.subplot(111)
    plt.subplots_adjust(left=0.115, right=0.88)
    ax.bar(x, y, width=0.6)

    # plt.plot(x, y, color="yellow")
    # fig = plt.gcf()
    plt.show()

if __name__ == "__main__":
    mongo_helper = MongoDBHelper('yelp')
    # my_query = mongo_helper.reviews.query().filter(Q(stars = 5) | Q(stars=4)).execute().dataframe()
    # four_star_reviews = mongo_helper.reviews.query().filter(stars = 4).count().execute().get()
    # print(my_query)
    text_only = mongo_helper.reviews.query().filter(stars = 4).projection(text=1, _id=0).execute().dataframe()
    print(text_only)
    # main()
