# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


class GubaPipeline:
    def __init__(self):
        # 连接配置信息
        config = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': '0816abcABC@root',
            'db': 'FIN_sentiment',
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor,
        }

        self.conn = pymysql.connect(**config)
        self.cursor = self.conn.cursor()

        # 如果无表则创建表
        queryline = "CREATE TABLE IF NOT EXISTS DC_TBL(" \
                    "Item_id INT UNSIGNED AUTO_INCREMENT," \
                    "stockCode VARCHAR(20) NOT NULL," \
                    "stockName VARCHAR(20) NULL," \
                    "readNum INT NOT NULL," \
                    "commentNum INT NOT NULL," \
                    "commentWriterID VARCHAR(50) NOT NULL," \
                    "commentWriter VARCHAR(50) NOT NULL," \
                    "writerInfluence INT NOT NULL," \
                    "writerUserAge VARCHAR(20) NOT NULL," \
                    "commentTime DATETIME NOT NULL," \
                    "writerIsMajia INT NOT NULL," \
                    "writerIsBackType INT NOT NULL," \
                    "commentURL VARCHAR(300) NULL," \
                    "commentTitle TEXT(300) NULL," \
                    "commentContent TEXT(30000) NULL," \
                    "commentLikeCount INT NOT NULL," \
                    "replyInfo TEXT(5000) NULL," \
                    "source VARCHAR(100) NULL," \
                    "TIME_CREATED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," \
                    "PRIMARY KEY (Item_id))ENGINE=InnoDB DEFAULT CHARSET=utf8"

        self.cursor.execute(queryline)
        self.conn.commit()

    def open_spider(self, spider):
        if spider.name == 'createEastMoneySpider':
            # 清空数据库
            # self.cursor.execute("TRUNCATE TABLE DC_TBL")
            # self.conn.commit()
            pass

    def process_item(self, item, spider):

        if spider.name == 'createEastMoneySpider':
            stockCode = item['stockCode']
            stockName = item['stockName']
            readNum = item['readNum']
            commentNum = item['commentNum']

            commentWriterID = item['commentWriterID']
            commentWriter = item['commentWriter']
            commentTime = item['commentTime']
            commentURL = item['commentURL']
            source = item['source']

            writerInfluence = item['writerInfluence']
            writerUserAge = item['writerUserAge']
            writerIsMajia = item['writerIsMajia']
            writerIsBackType = item['writerIsBackType']

            commentTitle = item['commentTitle']
            commentContent = item['commentContent'][0:30000]
            commentLikeCount = item['commentLikeCount']
            replyInfo = item['replyInfo']

            # 执行sql语句，插入记录
            sql = "INSERT INTO DC_TBL(stockCode, stockName, readNum, commentNum, commentWriterID, commentWriter," \
                  "commentTime, commentURL, source, writerInfluence, writerUserAge, writerIsMajia , writerIsBackType, " \
                  " commentTitle, commentContent, commentLikeCount, replyInfo) VALUES " \
                  "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            self.cursor.execute(sql, (
                stockCode, stockName, readNum, commentNum, commentWriterID, commentWriter, commentTime, commentURL,
                source, writerInfluence, writerUserAge, writerIsMajia, writerIsBackType, commentTitle, commentContent,
                commentLikeCount, replyInfo))
            self.conn.commit()
            return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
