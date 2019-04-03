# -*- coding: utf-8 -*-
import scrapy


class TestSpiderSpider(scrapy.Spider):
    name = 'test_spider'
    base_url="https://www.baidu.com/s?wd="

    def start_requests(self):
        for i in range(10):
            yield scrapy.Request(self.base_url+str(i),callback=self.parse)

        for i in range(100):
            yield scrapy.Request(self.base_url + str(i), callback=self.parse)

    def parse(self, response):
        self.logger.debug("url",response.url)
