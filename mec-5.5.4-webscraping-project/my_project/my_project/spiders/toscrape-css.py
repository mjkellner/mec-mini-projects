# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 14:34:22 2023

@author: t820608
"""

import scrapy

class QuotesSpider(scrapy.Spider):
    name = "toscrape-css"

    def start_requests(self):
        url = 'http://quotes.toscrape.com/'
        tag = getattr(self, 'tag', None)
        if tag is not None:
            url = url + 'tag/' + tag
        yield scrapy.Request(url, self.parse)

    def parse(self, response):
        author_page_links = response.css('.author + a')
        yield from response.follow_all(author_page_links, self.parse_author)

        pagination_links = response.css('li.next a')
        yield from response.follow_all(pagination_links, self.parse)

    def parse_author(self, response):
        def extract_with_css(query):
            return response.css(query).get(default='').strip()
        
        yield {
            'name': extract_with_css('h3.author-title::text'),
            'birth_date': extract_with_css('.author-born-date::text'),
            'birth_location': extract_with_css('.author-born-location::text'),
            'bio': extract_with_css('.author-description::text'),
        }
        
        ## given a tag, get the author details including name, birthdate, birth location and bio