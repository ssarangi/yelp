__author__ = 'sarangis'

from mongodb import *
import matplotlib.pyplot as plt

if __name__ == "__main__":
    mongo_helper = MongoDBHelper('yelp')
    # query = mongo_helper.users.query().filter(average_stars__gt = 2).filter(votes__funny__gt = 2)
    # print(mongo_helper.users.execute_query(query).dataframe())
    query_5_star = mongo_helper.reviews.query().filter(stars = 5).count()
    five_star_review = mongo_helper.reviews.execute_query(query_5_star).count()

    query_4_star = mongo_helper.reviews.query().filter(stars = 4).count()
    four_star_review = mongo_helper.reviews.execute_query(query_4_star).count()

    query_3_star = mongo_helper.reviews.query().filter(stars = 3).count()
    three_star_review = mongo_helper.reviews.execute_query(query_3_star).count()

    query_2_star = mongo_helper.reviews.query().filter(stars = 2).count()
    two_star_review = mongo_helper.reviews.execute_query(query_2_star).count()

    query_1_star = mongo_helper.reviews.query().filter(stars = 1).count()
    one_star_review = mongo_helper.reviews.execute_query(query_1_star).count()

    y = [one_star_review, two_star_review, three_star_review, four_star_review, five_star_review]
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
