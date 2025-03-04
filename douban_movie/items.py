# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class DoubanMovieItem(scrapy.Item):
    movie_name = scrapy.Field()
    comment = scrapy.Field()
    rating = scrapy.Field()
    username = scrapy.Field()
