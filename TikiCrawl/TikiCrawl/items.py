# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TikicrawlItem(scrapy.Item):
    item_id = scrapy.Field()
    name_product = scrapy.Field()
    price = scrapy.Field()
    rating_average = scrapy.Field()
    review_count = scrapy.Field()
    description = scrapy.Field()
    url = scrapy.Field()
    quantity_sold = scrapy.Field()
    brand = scrapy.Field()
    category = scrapy.Field()


