# -*- coding: utf-8 -*-
import os
import csv
import glob
import MySQLdb
import scrapy


class BooksSpider(scrapy.Spider):
    name = 'books'
    allowed_domains = ['books.toscrape.com']
    start_urls = ['http://books.toscrape.com']

    def parse(self, response):
        books = response.xpath('//*[@class="col-xs-6 col-sm-4 col-md-3 col-lg-3"]')
        for book in books:
            url = response.urljoin(book.xpath('.//*[@class="product_pod"]/h3/a/@href').extract_first())

            yield scrapy.Request(url,
                                 callback=self.parse_book,
                                 meta={'url': url})

        next_page_url = response.xpath('//*[@class="next"]/a/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(response.urljoin(next_page_url), callback=self.parse)

    def parse_book(self, response):
        url = response.meta['url']

        title = response.xpath('//h1/text()').extract_first()
        price = response.xpath('(//p[@class="price_color"]/text())[1]').extract_first()
        description = response.xpath('//*[@id="content_inner"]/article/p/text()').extract()

        yield{'Title': title,
              'Price': price,
              'Url': url,
              'Description': description}

    def close(self, reason):
        csv_file = max(glob.iglob('*.csv'), key=os.path.getctime)

        mydb = MySQLdb.connect(host='localhost',
                               user='ktm',
                               passwd='2486',
                               db='books_db')
        cursor = mydb.cursor()

        csv_data = csv.reader(file(csv_file))

        row_count = 0
        for row in csv_data:
            if row_count != 0:
                cursor.execute('INSERT IGNORE INTO books_table(url, description, title) VALUES(%s, %s, %s, %s)', row)
            row_count += 1

        mydb.commit()
        cursor.close()