# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 11:34:25 2023

@author: t820608
"""

import scrapy


class QuotesSpider(scrapy.Spider):
    name = "myspider5"
    start_urls = [
        'http://quotes.toscrape.com/page/1/',
    ]

    def parse(self, response):
        for quote in response.css('div.quote'):
            yield {
                'text': quote.css('span.text::text').get(),
                'author': quote.css('span small::text').get(),
                'tags': quote.css('div.tags a.tag::text').getall(),
            }

        yield from response.follow_all(css='ul.pager a', callback=self.parse)