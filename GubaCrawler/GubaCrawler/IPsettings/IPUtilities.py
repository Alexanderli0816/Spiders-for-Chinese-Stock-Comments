# !/usr/bin/python3.6
# -*- coding: UTF-8 -*-
# @author: guichuan

import requests
import os
from scrapy.selector import Selector
import pandas as pd
import numpy as np
import datetime
import random
import multiprocessing as mp
import time
import warnings


def crawl_xici_ip(item_count, fileName='xici_iplist.par'):
    """
    爬取西刺item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_xici_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}

    count = 1
    page_count = 0
    ip_list = []
    while count <= item_count:
        response = requests.get(url='http://www.xicidaili.com/nn/{0}'.format(page_count + 1), headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('//*[@id="ip_list"]//tr')

        for tr in all_trs[1:]:
            ip = tr.xpath('td[2]/text()').extract_first()
            port = tr.xpath('td[3]/text()').extract_first()
            ip_type = tr.xpath('td[6]/text()').extract_first().lower()
            ip_speed = tr.xpath('td[7]/div/@title').extract_first()
            if ip_speed:
                ip_speed = float(ip_speed.split(u'秒')[0])
            ip_time = tr.xpath('td[10]/text()').extract_first()
            ip_time = str(datetime.datetime.strptime('20' + ip_time, '%Y-%m-%d %H:%M'))

            count += 1
            ip_this = '{0}://{1}:{2}'.format(ip_type, ip, port)
            status, _ = IPUtil.judge_ip(ip_this)
            if status:
                print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of xici.'.format(ip_type, ip, port, count))
                ip_list.append((ip, port, ip_type, ip_speed, ip_time))

            if count >= item_count:
                break

        else:
            page_count += 1

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


def crawl_89_ip(item_count, fileName='89_iplist.par'):
    """
    爬取89ip网站item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_89_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}

    count = 1
    page_count = 0
    ip_list = []
    while count <= item_count:
        response = requests.get(url='http://www.89ip.cn/index_{}.html'.format(page_count + 1), headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('//table[@class="layui-table"]//tbody//tr')

        for tr in all_trs[0:]:
            ip = tr.xpath('td[1]/text()').extract_first().strip()
            port = tr.xpath('td[2]/text()').extract_first().strip()
            ip_type = 'http'
            ip_speed = np.nan
            ip_time = tr.xpath('td[5]/text()').extract_first().strip()
            ip_time = datetime.datetime.strptime(ip_time, '%Y/%m/%d %H:%M:%S')

            count += 1

            ip_this = '{0}://{1}:{2}'.format(ip_type, ip, port)
            status, _ = IPUtil.judge_ip(ip_this, verbose=False)
            if status:
                print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of 89ip.'.format(ip_type, ip, port, count))
                ip_list.append((ip, port, ip_type, ip_speed, ip_time))

            if count >= item_count:
                break

        else:
            if len(all_trs) == 0:
                break
            else:
                page_count += 1

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


def crawl_kuaidaili_ip(item_count, fileName='kuaidaili_iplist.par'):
    """
    爬取kuaidaili网站item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_kuaidaili_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}

    count = 1
    page_count = 0
    ip_list = []
    while count <= item_count:
        response = requests.get(url='https://www.kuaidaili.com/free/inha/{}/'.format(page_count + 1), headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('//table[@class="table table-bordered table-striped"]//tbody//tr')

        for tr in all_trs[1:]:
            ip = tr.xpath('td[1]/text()').extract_first().strip()
            port = tr.xpath('td[2]/text()').extract_first().strip()
            ip_type = 'http'
            ip_speed = np.nan
            ip_time = tr.xpath('td[7]/text()').extract_first().strip()
            ip_time = datetime.datetime.strptime(ip_time, '%Y-%m-%d %H:%M:%S')

            count += 1

            ip_this = '{0}://{1}:{2}'.format(ip_type, ip, port)
            status, _ = IPUtil.judge_ip(ip_this, verbose=False)
            if status:
                print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of kuaidaili.'.format(ip_type, ip, port, count))
                ip_list.append((ip, port, ip_type, ip_speed, ip_time))

            if count >= item_count:
                break

        else:
            if len(all_trs) == 0:
                break
            else:
                page_count += 1

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


def crawl_xiladaili_ip(item_count, fileName='xiladaili_iplist.par'):
    """
    爬取xiladaili网站item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_xiladaili_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}

    count = 1
    page_count = 0
    ip_list = []
    while count <= item_count:
        response = requests.get(url='https://www.kuaidaili.com/free/inha/{}/'.format(page_count + 1), headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('//table[@class="table table-bordered table-striped"]//tbody//tr')

        for tr in all_trs[1:]:
            ip = tr.xpath('td[1]/text()').extract_first().strip()
            port = tr.xpath('td[2]/text()').extract_first().strip()
            ip_type = 'http'
            ip_speed = np.nan
            ip_time = tr.xpath('td[7]/text()').extract_first().strip()
            ip_time = datetime.datetime.strptime(ip_time, '%Y-%m-%d %H:%M:%S')

            count += 1

            ip_this = '{0}://{1}:{2}'.format(ip_type, ip, port)
            status, _ = IPUtil.judge_ip(ip_this, verbose=False)
            if status:
                print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of xiladaili.'.format(ip_type, ip, port, count))
                ip_list.append((ip, port, ip_type, ip_speed, ip_time))

            if count >= item_count:
                break

        else:
            if len(all_trs) == 0:
                break
            else:
                page_count += 1

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


def crawl_ip3366_ip(item_count, fileName='ip3366_iplist.par'):
    """
    爬取89ip网站item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_ip3366_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}

    count = 1
    page_count = 0
    ip_list = []
    while count <= item_count:
        response = requests.get(url='http://www.ip3366.net/free/?stype=1&page={}'.format(page_count + 1),
                                headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('//table[@class="table table-bordered table-striped"]//tbody//tr')

        for tr in all_trs:
            ip = tr.xpath('td[1]/text()').extract_first().strip()
            port = tr.xpath('td[2]/text()').extract_first().strip()
            ip_type = tr.xpath('td[4]/text()').extract_first().strip().lower()
            ip_speed = tr.xpath('td[6]/text()').extract_first().strip()
            ip_time = tr.xpath('td[7]/text()').extract_first().strip()
            ip_time = datetime.datetime.strptime(ip_time, '%Y/%m/%d %H:%M:%S')

            count += 1

            ip_this = '{0}://{1}:{2}'.format(ip_type, ip, port)
            status, _ = IPUtil.judge_ip(ip_this, verbose=False)
            if status:
                print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of ip3366.'.format(ip_type, ip, port, count))
                ip_list.append((ip, port, ip_type, ip_speed, ip_time))

            if count >= item_count:
                break

        else:
            if len(all_trs) == 0:
                break
            else:
                page_count += 1

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


def crawl_66ip_ip(item_count, fileName='66ip_iplist.par'):
    """
    爬取89ip网站item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_66ip_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}

    count = 1
    ip_list = []
    while count <= item_count:
        response = requests.get(url='http://www.66ip.cn/nmtq.php?getnum=300&isp=0&anonymoustype=3'
                                    '&start=&ports=&export=&ipaddress=&area=1&proxytype=0&api=66ip', headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('/html/body//text()').extract()

        for item in all_trs:
            count += 1
            ip_this = item.strip()
            try:
                ip, port = ip_this.split(':')
            except ValueError:
                print(ip_this)
                continue

            status, _ = IPUtil.judge_ip(ip_this, verbose=False)
            if status:
                print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of 66ip.'.format('http', ip, port, count))
                ip_list.append((ip, port, 'http', np.nan, np.nan))

            if count >= item_count:
                break

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


# def crawl_ihuan_ip(item_count, fileName='ihuan_iplist.par'):
#     """
#     爬取89ip网站item_count个数量的ip,存为parquet格式
#     :return:
#     """
#     print("\nCrawling_ihun_ip...")
#     path = os.path.join(os.path.abspath(os.curdir))
#     file_name = os.path.join(path, fileName)
#
#     headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0",
#                "Content-Type": "application/x-www-form-urlencoded",
#                "Upgrade-Insecure-Requests": "1",
#                "Host": "ip.ihuan.me",
#                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#                "Accept-Language": "en-US,en;q=0.5",
#                "Accept-Encoding": "gzip,deflate",
#                "Referer": "https://ip.ihuan.me/ti.html",
#                "Pragma": "no-cache",
#                "Cache-Control": "no-cache"
#                }
#     data = {"anonymity": "2",
#             "num": "3000",
#             "sort": "1",
#             "type": "0",
#             "key": "f8abe06f9dcb96776b8c16d9eb5cc0a2"
#             }
#
#     cookie_dict = {
#         "__cfduid": "d24ab976aab68fc34226818a648cafc611591800745",
#         "Hm_lpvt_8ccd0ef22095c2eebfe4cd6187dea829": "1592974423",
#         "Hm_lvt_8ccd0ef22095c2eebfe4cd6187dea829": "1591800750, 1592968177",
#         "statistics": "8689a6d889b430723559d3e9ae450e00"}
#     count = 1
#     ip_list = []
#     while count <= item_count:
#         response = requests.post(url='https://ip.ihuan.me/tqdl.html', headers=headers, data=data, cookies=cookie_dict)
#         time.sleep(0.05)
#         all_trs = Selector(text=response.text).xpath('/html/body//text()').extract()
#
#         for item in all_trs:
#             count += 1
#             ip_this = item.strip()
#             ip, port = ip_this.split(':')
#             status, _ = IPUtil.judge_ip(ip_this, verbose=False)
#             if status:
#                 print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item'.format('http', ip, port, count))
#                 ip_list.append((ip, port, 'http', np.nan, np.nan))
#
#             if count >= item_count:
#                 break
#
#     df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
#     df.to_parquet(file_name)


def crawl_ihuan_ip(item_count, fileName='ihuan_iplist.par'):
    """
    爬取89ip网站item_count个数量的ip,存为parquet格式
    :return:
    """
    print("Crawling_ihun_ip...")
    path = os.path.join(os.path.abspath(os.curdir))
    file_name = os.path.join(path, fileName)

    headers = {"User-Agent": "Mozilla/5.0(X11;Linux x86_64;rv:60.0) Gecko/20100101Firefox/60.0"}
    today = datetime.datetime.today()
    today = datetime.datetime(today.year, today.month, today.day, today.hour)

    count = 1
    ip_list = []
    finish_flag = False
    while finish_flag is not True:
        response = requests.post(url='https://ip.ihuan.me/today/{}/{:02d}/{:02d}/{:02d}.html'.
                                 format(today.year, today.month, today.day, today.hour), headers=headers)
        time.sleep(0.05)
        all_trs = Selector(text=response.text).xpath('//p[@class="text-left"]//text()').extract()

        for item in all_trs:
            item = item.strip()
            if item.split('@')[-1].split('#')[0] == 'HTTP' and item.split('@')[-1].split('#')[1][1:3] == "高匿":
                count += 1
                try:
                    ip_this = item.split('@')[0]
                    ip, port = ip_this.split(':')
                except ValueError:
                    continue

                status, _ = IPUtil.judge_ip(ip_this, verbose=False)
                if status:
                    print('>>>>>  Get proxy : {0}://{1}:{2}, from {3}th item of ihuan.'.format('http', ip, port, count))
                    ip_list.append((ip, port, 'http', np.nan, np.nan))

                if count >= item_count:
                    finish_flag = True
                    break
            else:
                continue
        today = today - datetime.timedelta(hours=1)

    df = pd.DataFrame(ip_list, columns=['ip', 'port', 'type', 'speed', 'aliveTime'])
    df.to_parquet(file_name)


class IPUtil(object):
    # noinspection SqlDialectInspection

    def __init__(self, sources: tuple = ('xici', '89ip', 'kuaidaili', 'xiladaili', '66ip', 'ip3366', 'ihuan')):
        source_dict = {'xici': 'xici_iplist.par',
                       '89ip': '89_iplist.par',
                       'kuaidaili': 'kuaidaili_iplist.par',
                       'xiladaili': 'xiladaili_iplist.par',
                       '66ip': '66ip_iplist.par',
                       'ip3366': 'ip3366_iplist.par',
                       'ihuan': 'ihuan_iplist.par'
                       }

        path = os.path.split(os.path.realpath(__file__))[0]
        df_list = []
        for source in sources:
            try:
                df_list.append(pd.read_parquet(os.path.join(path, source_dict[source])))
            except Exception as err:
                print('source %s is missing!' % err)
                continue
        df = pd.concat(df_list)
        IP_POOL = list(zip(df['ip'].astype(str).tolist(),
                           df['port'].astype(str).tolist(),
                           df['type'].astype(str).tolist()))
        ip_all = list(set(["{0}://{1}:{2}".format(ip_type, ip, port) for ip, port, ip_type in IP_POOL
                           if ip_type.lower() == 'http']))

        pool = mp.Pool(processes=8)
        connect_test_list_obj = [pool.apply_async(IPUtil.judge_ip, [item, 5]) for item in ip_all]
        pool.close()
        pool.join()
        ip_all_checked = []
        for item in connect_test_list_obj:
            status, ip_result = item.get()
            if status:
                ip_all_checked.append(ip_result)

        self.IP_POOL = ip_all_checked
        print("\n>>>>>  Get proxy IP list with length {}".format(len(self.IP_POOL)))

    def random(self, check=False):
        ip_random = random.choice(self.IP_POOL)

        if check:
            judge_re = IPUtil.judge_ip(ip_random, timeout=5)
            if judge_re:
                return ip_random
            else:
                return self.random()
        else:
            return ip_random

    def remove(self, ip):
        self.IP_POOL.remove(ip)

    @staticmethod
    def judge_ip(proxy_url, timeout=10, verbose=False):
        warnings.filterwarnings('ignore')
        # 判断ip是否可用，如果通过代理ip访问百度，返回code200则说明可用
        http_url = "http://guba.eastmoney.com/news,600519,937492931_1.html"
        try:
            proxy_dict = {
                "http": proxy_url,
            }
            requests.adapters.DEFAULT_RETRIES = 5
            response = requests.get(http_url, proxies=proxy_dict, headers={'Connection': 'close'}, verify=False,
                                    timeout=timeout)
            test_string = Selector(text=response.text).xpath(
                '/html/body/div[5]/div[3]/div[5]/div[1]/div[3]/div[1]/strong/a/font/text()').extract_first()
        except Exception as err:
            # print('>>>>>  Check proxy : {}, Err: {}'.format(proxy_url, err))
            return False, proxy_url

        else:
            code = response.status_code
            if 200 <= code < 300 and test_string == 'Wan2':
                if verbose:
                    print('>>>>>  Check proxy : {}, return code: {}'.format(proxy_url, code))
                return True, proxy_url
            else:
                return False, proxy_url


if __name__ == '__main__':
    IPU = IPUtil()
    print(IPU.random())

    # func_pool = [crawl_xici_ip, crawl_89_ip, crawl_kuaidaili_ip, crawl_xiladaili_ip, crawl_ip3366_ip, crawl_66ip_ip, crawl_ihuan_ip]
    # para = [1000, 750, 1000, 100, 1000, 300, 2000]
    # iter_items = zip(func_pool, para)
    #
    # for func, item_count in iter_items:
    #     p = Process(target=func, args=(item_count,))
    #     p.start()
