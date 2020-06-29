# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urljoin
from GubaCrawler.items import GubaItem
import pandas as pd
import os
import json
import re
from scrapy_splash import SplashRequest
import requests
from scrapy.selector import Selector
import datetime
import time

urlbase = "http://guba.eastmoney.com/"
stock_list_file_name = 'GubaCrawler/stock_lists/ZZ500.csv'


def format_string(string):
    try:
        return re.sub('\\s', "", string)
    except Exception:
        return string


class EastMoneySpider(scrapy.Spider):
    name = 'createEastMoneySpider'
    today = datetime.datetime.fromordinal(datetime.date(2020, 6, 22).toordinal())
    updateTimeRange = (today + datetime.timedelta(days=-100), today)

    lua_script = \
        '''function main(splash, args)
            splash.media_source_enabled = false
            splash.images_enabled=false
            splash.js_enabled=true
            splash:on_request(function(request)
            request:set_proxy{
                                host = args.host,
                                port = args.port,
                                username = '',
                                password = '',
                                type = "HTTP",
                              }
            if string.find(request.url, "HotKeyword") ~= nil 
               or string.find(request.url, "GubaCalendars") ~= nil 
               or string.find(request.url, "newspush") ~= nil
            then 
                  request.abort()
                end
            end)
            assert(splash:go(args.url))
            assert(splash:wait(1))
            return {
              html=splash:html()
                  }
            end
        '''

    def __init__(self, stock_list=None, *args, **kwargs):
        super(EastMoneySpider, self).__init__(*args, **kwargs)
        self.stock_list = stock_list

    @staticmethod
    def get_stock_list():
        spider_path = os.path.abspath(os.curdir)
        df = pd.read_csv(os.path.join(spider_path, stock_list_file_name),
                         dtype={0: str, 1: str}, header=0)
        stock_dictionary = dict(zip(df['ticker'].tolist(), df['display_name'].tolist()))
        return stock_dictionary

    def start_requests(self):
        stock_dictionary = EastMoneySpider.get_stock_list()
        for code, name in stock_dictionary.items():
            if code in self.stock_list:
                url = 'http://guba.eastmoney.com/list,{},f_1.html'.format(code)
                request = SplashRequest(url, callback=self.parse, endpoint='execute',
                                        args={'lua_source': EastMoneySpider.lua_script}, dont_filter=True)
                request.meta['stockCode'] = code
                request.meta['stockName'] = name
                request.meta['page'] = 1
                yield request

    def parse(self, response):
        """
        信息第一步处理方法, 根据股票代码和股票名称生成以下数据:
        readNum: 阅读数量
        commentNum: 评论数量
        commentURL: 超链接地址
        """
        selector = response.xpath(
            '//div[@id="articlelistnew"]//div[@class="articleh normal_post" or @class="articleh normal_post odd"]')
        if len(selector.extract()) > 0:
            page = response.meta['page']
            # 多空情绪request url
            # http://gubacdn.dfcfw.com/LookUpAndDown/sz300059.js?

            for line in selector:
                readNum = line.xpath('./span[@class="l1 a1"]/text()').extract_first()
                commentNum = line.xpath('./span[@class="l2 a2"]/text()').extract_first()
                comments_href = urljoin(urlbase, line.xpath('./span[@class="l3 a3"]/a/@href').extract_first())

                # target_url = "http://guba.eastmoney.com/news,300059,933697446_1.html#storeply"
                comments_href_temp = comments_href.split('.')[0:-1]
                comments_href_temp = '.'.join(comments_href_temp)
                comments_href = ''.join([comments_href_temp, '_1.html'])

                # 进入连接爬取评论
                request = SplashRequest(comments_href, callback=self.parse_comments, endpoint='execute',
                                        args={'lua_source': EastMoneySpider.lua_script})
                request.meta['stockCode'] = response.meta['stockCode']
                request.meta['stockName'] = response.meta['stockName']
                if '万' in readNum:
                    request.meta['readNum'] = float(readNum[:-1]) * 10000
                else:
                    request.meta['readNum'] = readNum
                request.meta['commentNum'] = commentNum
                request.meta['reply_page'] = 1
                yield request

            else:
                # 获取下一页链接
                page += 1
                if EastMoneySpider.get_page_end_time(comments_href) >= EastMoneySpider.updateTimeRange[0]:
                    next_page_link = 'http://guba.eastmoney.com/list,{},f_{}.html'.format(response.meta['stockCode'],
                                                                                          page)
                    request = SplashRequest(next_page_link, callback=self.parse, endpoint='execute',
                                            args={'lua_source': EastMoneySpider.lua_script})
                    request.meta['stockCode'] = response.meta['stockCode']
                    request.meta['stockName'] = response.meta['stockName']
                    request.meta['page'] = page
                    yield request

        else:
            print('股票{}已经到达最后一页, 共{}页！'.format(response.meta['stockCode'], response.meta['page'] - 1))
            return

    def parse_comments(self, response):
        """
        信息第二步处理方法
        生成标准化数据:
            stockCode: 股票代码
            stockName: 股票名称
            readNum: 阅读数量
            commentNum: 评论数量
            commentTitle: 评论标题
            commentWriter：评论作者
            commentTime: 评论时间
            commentURL: 超链接地址
            source：数据来源
        """
        reply_page = response.meta['reply_page']

        Guba_info = GubaItem()
        Guba_info['source'] = 'DFCF'
        comments_href = EastMoneySpider.handle_url(str(response.request))
        Guba_info['commentURL'] = comments_href
        Guba_info['stockCode'] = response.meta['stockCode']
        Guba_info['stockName'] = response.meta['stockName']
        Guba_info['readNum'] = response.meta['readNum']
        Guba_info['commentNum'] = response.meta['commentNum']

        if reply_page == 1:
            # 进入comments第一页需要爬取作者信息
            selector = response.xpath('//*[@id="zwcontent"]')

            # 评论者相关
            try:
                data_json = json.loads(selector.xpath('./div[@id="zwcontt"]/div/@data-json').extract_first())
                Guba_info['commentWriterID'] = data_json['user_id']
                Guba_info['commentWriter'] = EastMoneySpider.handle_non_utf_string(data_json['user_nickname'])
                Guba_info['writerInfluence'] = data_json['user_influ_level']
                Guba_info['writerUserAge'] = data_json['user_age']
                Guba_info['writerIsMajia'] = int(data_json['user_is_majia'])
                Guba_info['writerIsBackType'] = data_json['user_black_type']
            except TypeError:
                yield response.request
                return

            # 评论时间
            string = selector.xpath('.//div[@class="zwfbtime"]/text()').extract_first()
            try:
                Guba_info['commentTime'] = ' '.join(string.split(' ')[1:3])
            except AttributeError:
                # print('Found empty commentTime: %s, retrying...' % comments_href)
                yield response.request
                return

            # 评论内容
            commentTitle = format_string(selector.xpath('.//div[@id="zwconttbt"]')
                                         .extract_first())
            Guba_info['commentTitle'] = EastMoneySpider.handle_reply(commentTitle)

            string_list = selector.xpath('.//div[@class="stockcodec .xeditor"]//text()').extract()

            if len(selector.xpath('.//div[@class="zwzfc_cont stockcodec"]')) > 0:
                string_zf_list = selector.xpath('.//div[@class="zwzfc_title" '
                                                'or @class="zwzfc_cont stockcodec"]'
                                                '//text()').extract()
                string_zf = ''.join([item.strip() for item in string_zf_list])
                string_comment = ''.join([item.strip() for item in string_list])
                Guba_info['commentContent'] = "[{}]".format(string_comment) + string_zf
            else:
                Guba_info['commentContent'] = format_string(''.join([item.strip() for item in string_list]))

            Guba_info['commentLikeCount'] = selector.xpath('//*[@id="like_wrap"]/@data-like_count').extract_first()

        else:
            Guba_info['commentWriterID'] = response.meta['commentWriterID']
            Guba_info['commentWriter'] = response.meta['commentWriter']
            Guba_info['writerInfluence'] = response.meta['writerInfluence']
            Guba_info['writerUserAge'] = response.meta['writerUserAge']
            Guba_info['writerIsMajia'] = response.meta['writerIsMajia']
            Guba_info['writerIsBackType'] = response.meta['writerIsBackType']
            Guba_info['commentTime'] = response.meta['commentTime']
            Guba_info['commentTitle'] = response.meta['commentTitle']
            Guba_info['commentContent'] = response.meta['commentContent']
            Guba_info['commentLikeCount'] = response.meta['commentLikeCount']

        last_comment_time = datetime.datetime.strptime(Guba_info['commentTime'], '%Y-%m-%d %H:%M:%S')
        if EastMoneySpider.updateTimeRange[0] <= last_comment_time <= EastMoneySpider.updateTimeRange[1]:
            selector_reply = response.xpath('//div[@id="comment_all_content"]//div[@id="zwlist"]//div[@class="level1_item clearfix" and @data-reply_id]')
            if len(selector_reply.extract()) > 0:
                for item in selector_reply:
                    item_info = dict()
                    # 回复者相关
                    try:
                        data_json = json.loads(item.xpath('.//span[@class="jv"]/@data-userinfo').extract_first())
                        item_info['replyWriterID'] = data_json['user_id']
                        item_info['replyWriter'] = EastMoneySpider.handle_non_utf_string(data_json['user_nickname'])
                        item_info['replyInfluence'] = data_json['user_influ_level']
                        item_info['replyUserAge'] = data_json['user_age']
                        item_info['replyIsMajia'] = int(data_json['user_is_majia'])
                        item_info['replyIsBackType'] = data_json['user_black_type']
                    except TypeError:
                        yield response.request
                        return

                    # 回复时间
                    string = item.xpath('.//div[@class="publish_time"]/text()').extract_first()
                    try:
                        item_info['replyTime'] = ' '.join(string.split(' ')[1:3])
                    except AttributeError:
                        yield response.request
                        return

                    # 评论内容
                    string = item.xpath('.//div[@class="level1_reply_cont"]//div[@class="short_text"]').extract_first()
                    string = EastMoneySpider.handle_reply(string)
                    item_info['replyContent'] = string

                    item_info['replyLikeCount'] = item.xpath('./@data-reply_like_count').extract_first()
                    replyID = item.xpath('./@data-reply_id').extract_first()
                    item_info['replyID'] = replyID
                    if len(item.xpath('.//div[@class="level2_list" and @style="display:;"]').extract()) > 0:
                        # 有回复
                        selector_reply_reply = selector_reply.xpath('.//div[@class="level2_list" '
                                                                    'and @style="display:;"]//div[@class="level2_item"]')
                        item_info['ReplyToReply'] = dict()
                        for item_level2 in selector_reply_reply:
                            dict_temp = EastMoneySpider.get_rely_to_rely(item_level2)
                            item_info['ReplyToReply'] = dict_temp
                            Guba_info['replyInfo'] = json.dumps(item_info, ensure_ascii=False)
                            yield Guba_info

                    else:
                        item_info['ReplyToReply'] = None
                        Guba_info['replyInfo'] = json.dumps(item_info, ensure_ascii=False)
                        yield Guba_info

                # 获取下一页链接

                if len(response.xpath('//div[@class="pager talc zwpager"]//span[@class="jump_page"]').extract()) == 0:
                    pass

                else:
                    reply_page += 1

                    comments_href = EastMoneySpider.handle_url(str(response.request))
                    comments_href_temp = comments_href.split('_')[0:-1]
                    comments_href_temp = '.'.join(comments_href_temp)
                    next_page_link = ''.join([comments_href_temp, '_{}.html'.format(reply_page)])

                    request = SplashRequest(next_page_link, callback=self.parse_comments, endpoint='execute',
                                            args={'lua_source': EastMoneySpider.lua_script}, dont_filter=True)
                    request.meta['stockCode'] = Guba_info['stockCode']
                    request.meta['stockName'] = Guba_info['stockName']
                    request.meta['readNum'] = Guba_info['readNum']
                    request.meta['commentNum'] = Guba_info['commentNum']
                    request.meta['commentWriterID'] = Guba_info['commentWriterID']
                    request.meta['commentWriter'] = Guba_info['commentWriter']
                    request.meta['writerInfluence'] = Guba_info['writerInfluence']
                    request.meta['writerUserAge'] = Guba_info['writerUserAge']
                    request.meta['writerIsMajia'] = Guba_info['writerIsMajia']
                    request.meta['writerIsBackType'] = Guba_info['writerIsBackType']
                    request.meta['commentTime'] = Guba_info['commentTime']
                    request.meta['commentTitle'] = Guba_info['commentTitle']
                    request.meta['commentContent'] = Guba_info['commentContent']
                    request.meta['commentLikeCount'] = Guba_info['commentLikeCount']
                    request.meta['reply_page'] = reply_page
                    yield request

            elif reply_page == 1:
                Guba_info['replyInfo'] = json.dumps(dict(), ensure_ascii=False)
                yield Guba_info

            else:
                pass

        else:
            pass

    @staticmethod
    def handle_reply(string):
        flag_1 = 0
        flag_2 = 0
        flag_3 = 0
        pattern_1 = re.compile('<div class=\"([\\S]+)\">(\\s*)(.+)(\\s*)</div>', re.DOTALL)
        try:
            string = pattern_1.findall(string)[0][2]
        except IndexError:
            flag_1 = 1
        except TypeError:
            return None

        pattern_4 = re.compile('<div id=\"([\\S]+)\">(\\s*)(.+)(\\s*)</div>', re.DOTALL)
        try:
            string = pattern_4.findall(string)[0][2]
        except IndexError:
            flag_2 = 1

        pattern_5 = re.compile('<divid=\"([\\S]+)\">(\\s*)(.+)(\\s*)</div>', re.DOTALL)
        try:
            string = pattern_5.findall(string)[0][2]
        except IndexError:
            flag_3 = 1

        if flag_1 and flag_2 and flag_3:
            return None

        pattern_2 = re.compile('<img src=\"([\\S]+)\" title=\"([\\S]+)\">', re.DOTALL)
        matchObj = pattern_2.findall(string)
        while len(matchObj) > 0:
            string = re.sub(pattern_2, matchObj[0][1], string, count=1)
            matchObj = pattern_2.findall(string)

        pattern_3 = re.compile('<img title=\"([\\S]+)\" src=\"([\\S]+)\" alt=\"([\\S]+)\">', re.DOTALL)
        matchObj = pattern_3.findall(string)
        while len(matchObj) > 0:
            string = re.sub(pattern_3, matchObj[0][0], string, count=1)
            matchObj = pattern_3.findall(string)

        pattern_6 = re.compile('<span class=\"zwtitlepdf\">(\\s*)(.+)(\\s*)</span>', re.DOTALL)
        matchObj = pattern_6.findall(string)
        while len(matchObj) > 0:
            string = re.sub(pattern_6, matchObj[0][0], string, count=1)
            matchObj = pattern_6.findall(string)

        string = re.sub('\\s', "", string)
        return string

    @staticmethod
    def handle_reply_to_reply(string):
        pattern_1 = re.compile('<span class=\"([\\S]+)\">(\\s*)(.+)(\\s*)</span>', re.DOTALL)
        try:
            string = pattern_1.findall(string)[0][2]
        except TypeError:
            return None
        except IndexError:
            pass

        pattern_2 = re.compile('<img src=\"([\\S]+)\" title=\"([\\S]+)\">', re.DOTALL)
        matchObj = pattern_2.findall(string)
        while len(matchObj) > 0:
            string = re.sub(pattern_2, matchObj[0][1], string, count=1)
            matchObj = pattern_2.findall(string)

        pattern_3 = re.compile('<img title=\"([\\S]+)\" src=\"([\\S]+)\" alt=\"([\\S]+)\">', re.DOTALL)
        matchObj = pattern_3.findall(string)
        while len(matchObj) > 0:
            string = re.sub(pattern_3, matchObj[0][0], string, count=1)
            matchObj = pattern_3.findall(string)

        string = re.sub('\\s', "", string)
        return string

    @staticmethod
    def handle_url(string):
        pattern = re.compile('<GET ([\\S]+)(.+)(\\s*)via')
        string = pattern.findall(string)[0][0]
        return string

    @staticmethod
    def get_rely_to_rely(selector):
        item_info = dict()
        data_json = json.loads(selector.xpath('.//span[@class="jv"]/@data-userinfo').extract_first())
        item_info['replyWriterID'] = data_json['user_id']
        item_info['replyWriter'] = data_json['user_nickname']
        item_info['replyInfluence'] = data_json['user_influ_level']
        item_info['replyUserAge'] = data_json['user_age']
        item_info['replyIsMajia'] = int(data_json['user_is_majia'])
        item_info['replyIsBackType'] = data_json['user_black_type']

        # 回复时间
        item_info['replyTime'] = selector.xpath('.//span[@class="time fl"]/text()').extract_first()

        # 评论内容
        string = selector.xpath('.//span[@class="l2_short_text"]').extract_first()
        string = EastMoneySpider.handle_reply_to_reply(string)
        item_info['replyContent'] = string

        item_info['replyLikeCount'] = selector.xpath('./@data-reply_like_count').extract_first()
        replyID = selector.xpath('./@data-reply_id').extract_first()
        item_info['replyID'] = replyID
        return item_info

    @staticmethod
    def get_page_end_time(href):
        splash_url = 'http://localhost:8050/render.html'
        args = {'url': '{}'.format(href)}
        string = None
        while string is None:
            response = requests.get(splash_url, params=args)
            string = Selector(response).xpath('.//div[@class="zwfbtime"]/text()').extract_first()
            time.sleep(5)
        page_end_time = datetime.datetime.strptime(' '.join(string.split(' ')[1:3]), '%Y-%m-%d %H:%M:%S')
        return page_end_time

    @staticmethod
    def handle_non_utf_string(string):
        return re.sub("[^\u0020-\u9FA5]", "", string)
