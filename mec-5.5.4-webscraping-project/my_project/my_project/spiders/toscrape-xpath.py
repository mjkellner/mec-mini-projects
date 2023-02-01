# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 14:34:22 2023

@author: t820608
"""

import scrapy

class QuotesSpider(scrapy.Spider):
    name = "toscrape-xpath"

    start_urls = [
        'http://quotes.toscrape.com/page/1/',
        'http://quotes.toscrape.com/page/2/',
    ]

    def parse(self, response):
        
        for quote in response.css('div.quote'):
            yield {
                'text': (quote.xpath('//*[contains(concat(" ", @class, " "), "text")]/text()').get()).strip(),
                'author': (quote.xpath('//*[contains(concat(" ", @class, " "), "author")]/text()').get()).strip(),
            }
