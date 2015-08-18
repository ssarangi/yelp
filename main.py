__author__ = 'sarangis'

from mongodb import *
from bokeh.plotting import figure
from collections import OrderedDict
from bokeh.charts import Bar, output_file, show
import numpy as np

def main():
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

if __name__ == "__main__":
    mongo_helper = MongoDBHelper('yelp')
    # my_query = mongo_helper.reviews.query().filter(Q(stars = 5) | Q(stars=4)).execute().dataframe()
    # four_star_reviews = mongo_helper.reviews.query().filter(stars = 4).count().execute().get()
    # print(my_query)
    reviews_txt_df = mongo_helper.reviews.query().filter(stars = 4).projection(text=1, _id=0).execute().dataframe()
    col = reviews_txt_df["text"]

    txt = ""
    for line in col:
        txt += " " + line

    from wordcloud import WordCloud
    word_cloud = WordCloud().generate(txt)
    word_cloud.to_file("wordcloud.png")
    plot = figure(width=1000, height=1000)
    pix = np.array(word_cloud.to_image())
    plot.image_rgba(image=[pix], x=[0], y=[0], dw=[10], dh=[10])
    output_file("wordcloud.html")

    show(plot)
    # main()
