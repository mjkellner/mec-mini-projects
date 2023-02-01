# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import scrapy


class QuotesSpider(scrapy.Spider):
    name = "myspider2"
    start_urls = [
        'http://quotes.toscrape.com/page/1/',
        'http://quotes.toscrape.com/page/2/',
    ]

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').get(),
                'author': quote.css('small.author::text').get(),
                'tags': quote.css('div.tags a.tag::text').getall(),
            }
