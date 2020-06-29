# !/usr/bin/python3.6
# -*- coding: UTF-8 -*-
# @author: guichuan
from scrapy.utils.project import get_project_settings
import sys
path = r'/home/alex/桌面/Python/Project/Spider_Project/GubaCrawler'
sys.path.append(path)
from GubaCrawler.spiders.createEastMoneySpider import EastMoneySpider
from scrapy.crawler import CrawlerProcess
import os
import pandas as pd
import numpy as np
from multiprocessing import Process

stock_list_file_name = 'GubaCrawler/stock_lists/ZZ500.csv'


def get_stock_list():
    spider_path = os.path.abspath(os.curdir)
    df = pd.read_csv(os.path.join(spider_path, stock_list_file_name),
                     dtype={0: str, 1: str}, header=0)
    stock_dictionary = dict(zip(df['ticker'].tolist(), df['display_name'].tolist()))
    return stock_dictionary


def make_batches(size, batch_size):
    nb_batch = int(np.ceil(size / float(batch_size)))
    return [(i * batch_size, min(size, (i + 1) * batch_size)) for i in range(0, nb_batch)]


def spider_run(stock_list):
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(EastMoneySpider, stock_list=stock_list)
    process.start()


if __name__ == "__main__":
    stock_list = list(get_stock_list().keys())[200:250]
    query_list = make_batches(50, 25)
    for start, end in query_list:
        stock_list_query = stock_list[start:end]
        p = Process(target=spider_run, args=(stock_list_query,))
        p.start()
