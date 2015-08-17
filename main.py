__author__ = 'sarangis'

from mongodb import *
import matplotlib.pyplot as plt

def myfunc(object):
    print(object)

class Combine:
    def __init__(self, m, n):
        self.m = m
        self.n = n

    def __str__(self):
        return str(self.m) + "," + str(self.n)

class B:
    def __init__(self, m):
        self.m = m

    def __str__(self):
        return str(self.m)

    def __or__(self, other):
        print("OR method called")
        return Combine(self.m, other.m)

    def __and__(self, other):
        print("AND method called")
        return Combine(self.m, other.m)

def main():
    mongo_helper = MongoDBHelper('yelp')
    # query = mongo_helper.users.query().filter(average_stars__gt = 2).filter(votes__funny__gt = 2)
    # print(mongo_helper.users.execute_query(query).dataframe())
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

    fig = plt.figure()
    ax = plt.subplot(111)
    plt.subplots_adjust(left=0.115, right=0.88)
    ax.bar(x, y, width=0.6)

    # plt.plot(x, y, color="yellow")
    # fig = plt.gcf()
    plt.show()


if __name__ == "__main__":
    # main()
    # myfunc((B(3) & B(4)))
    # print(B(2) & B(4))
    main()