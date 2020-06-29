# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GubaItem(scrapy.Item):
    # define the fields for your item here like:
    # 爬取信息集
    stockCode = scrapy.Field()
    stockName = scrapy.Field()
    readNum = scrapy.Field()
    commentNum = scrapy.Field()

    commentWriterID = scrapy.Field()
    commentWriter = scrapy.Field()
    writerInfluence = scrapy.Field()
    writerUserAge = scrapy.Field()
    writerIsMajia = scrapy.Field()
    writerIsBackType = scrapy.Field()

    commentTime = scrapy.Field()
    commentURL = scrapy.Field()
    commentTitle = scrapy.Field()
    commentContent = scrapy.Field()
    commentLikeCount = scrapy.Field()
    replyInfo = scrapy.Field()

    source = scrapy.Field()
