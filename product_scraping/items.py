# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Product(scrapy.Item):
    store = scrapy.Field()
    barcodes = scrapy.Field()
    sku = scrapy.Field()
    brand = scrapy.Field()
    name = scrapy.Field()
    description = scrapy.Field()
    package = scrapy.Field()
    image_url = scrapy.Field()
    product = scrapy.Field()
    category = scrapy.Field()
    url = scrapy.Field()
    branch = scrapy.Field()
    stock = scrapy.Field()
    price = scrapy.Field()

    pass
