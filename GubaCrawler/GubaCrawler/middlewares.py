# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from fake_useragent import UserAgent
from .IPsettings.IPUtilities import IPUtil
import re
import time
import os


class GubadcSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class GubaDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    def __init__(self):
        self.ua = UserAgent()
        self.counter = 0

        self.use_proxy = True
        if self.use_proxy:
            self.IP_POOL = IPUtil()

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called

        self.counter += 1
        if self.counter % 2000 == 0:
            self.IP_POOL = IPUtil()
            os.system('curl -X POST http://localhost:8050/_gc')
            spider.logger.info('reset memory and splash.')

        request.headers['User-Agent'] = self.ua.random

        if self.use_proxy:
            request.headers['Connection'] = 'close'
            ip_this = self.IP_POOL.random()
            request.meta['splash']['args']['host'] = ip_this.split('//')[-1].split(':')[0]
            request.meta['splash']['args']['port'] = int(ip_this.split(':')[-1])
        # spider.logger.warn(request.meta)

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        if response.status != 200:
            self.counter += 1
            request.headers['User-Agent'] = self.ua.random
            request.headers['Connection'] = 'close'

            if self.use_proxy:
                ip_this = self.IP_POOL.random()
                request.meta['splash']['args']['host'] = ip_this.split('//')[-1].split(':')[0]
                request.meta['splash']['args']['port'] = int(ip_this.split(':')[-1])
            request.dont_filter = True
            time.sleep(5)
            pattern = re.compile('<GET ([\\S]+)(.+)(\\s*)via')
            string = pattern.findall(str(request))[0][0]
            spider.logger.warn('Request fails: %s | status: %s' % (string, str(response.status)))
            return request
        else:
            return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        self.counter += 1
        request.headers['User-Agent'] = self.ua.random
        request.headers['Connection'] = 'close'
        if self.use_proxy:
            ip_this = self.IP_POOL.random()
            request.meta['splash']['args']['host'] = ip_this.split('//')[-1].split(':')[0]
            request.meta['splash']['args']['port'] = int(ip_this.split(':')[-1])
        request.dont_filter = True
        time.sleep(30)
        pattern = re.compile('<GET ([\\S]+)(.+)(\\s*)via')
        string = pattern.findall(str(request))[0][0]
        spider.logger.warn('Request fails: %s, Error message: %s' % (string, exception))
        return request

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
        # command = 'docker run -d -p 8050:8050 --memory=30G --restart=always scrapinghub/splash --maxrss 29000 &'
        # os.system('echo %s|sudo -S %s' % (self.sudoPassword, command))
