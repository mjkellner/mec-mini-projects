# -*- coding: utf-8 -*-
"""
Created on Sat Feb 18 10:33:29 2023

@author: t820608
"""

import scrapy

class QuotesSpider(scrapy.Spider):
    name = "toscrape-xpath"
    start_urls = ['http://quotes.toscrape.com/']

    def parse(self, response):
        next_page_url = response.xpath('//li[@class="next"]/a/@href').extract_first()
        if next_page_url:
            next_page_abs_url = 'http://quotes.toscrape.com/' + next_page_url
            yield scrapy.Request(next_page_abs_url, self.parse)
            
        quotes = response.xpath('//div[@class="quote"]')
        
        for quote in quotes:
            author_partial_link = quote.xpath('.//span/a/@href').extract_first()    
            author_link = response.urljoin(author_partial_link)
            yield scrapy.Request(author_link, callback=self.parse_author)

    def parse_author(self, response):
        
        yield {
            'name': response.xpath('//h3[@class="author-title"]/text()').extract_first().strip(),
            'birth_date': response.xpath('//span[@class="author-born-date"]/text()').extract_first().strip(),
            'birth_location': response.xpath('//span[@class="author-born-location"]/text()').extract_first().strip()[3:],
            'bio': response.xpath('//div[@class="author-description"]/text()').extract_first().strip(),
        }
        
        ## given a tag, get the author details including name, birthdate, birth location and bio