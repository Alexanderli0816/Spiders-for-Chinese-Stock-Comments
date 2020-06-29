import scrapy
from GubaDC.items import GubaItem
from urllib.parse import urljoin
import random
from GubaDC.settings import IP_POOL

baseUrl = "http://guba.sina.com.cn"
startUrl = baseUrl + "/?s=bar&name={}&type=0&page=0"
stockCode = "sh600519"

class SinaSpider(scrapy.Spider):
    name = "sinaSpider"
    startPageNo = 0

    def start_requests(self):
        yield scrapy.Request(url=startUrl.format(stockCode))

    def parse(self, response):
        code = response.xpath('//div[@class="blk_stock_info clearfix"]//span[@class="bsit_code"]/text()').extract_first()
        code = code[1:-1]
        name = response.xpath('//div[@class="blk_stock_info clearfix"]//span[@class="bsit_name"]//a/text()').extract_first()

        for line in response.xpath('//div[@id="blk_list_02"]//tr[@class="tr2"]'):

            Guba_info = GubaItem()
            Guba_info['stockCode'] = code
            Guba_info['stockName'] = name
            Guba_info['Href'] = response.request.url
            Guba_info['Source'] = 'SINA'
            
            cols = line.xpath("./td")
            Guba_info['readNum'] = cols[0].xpath("./span/text()").extract_first()
            Guba_info['commentNum'] = cols[1].xpath("./span/text()").extract_first()
            titleSelector = cols[2].xpath("./a")
            Guba_info['Title'] = titleSelector.xpath("./text()").extract_first()
            Guba_info['Writer'] = cols[3].xpath("./div//a/text()").extract_first()
            if Guba_info['Writer'] is None:
                Guba_info['Writer'] = cols[3].xpath("./div/text()").extract_first()
            Guba_info['Time'] = cols[4].xpath('./text()').extract_first()
            yield Guba_info
            # print(Guba_info)

        for link in response.xpath('//div[@class="blk_01_b"]//p[@class="page"]//a'):
            request = scrapy.Request(urljoin(baseUrl, link.xpath("./@href").extract_first()))
            # useThisIp = random.choice(IP_POOL)
            # print('>>>>>  Using proxy : %s' % useThisIp["ip"])
            # request.meta["proxy"]= useThisIp["ip"]
            yield request
            

