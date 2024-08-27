import scrapy
import sqlite3
import logging
import json
import os
from jsonschema import validate, ValidationError


class TelegraphSpider(scrapy.Spider):
    name = "telegraph"
    allowed_domain = ["telegraph.bg/"]
    start_urls = ["https://telegraph.bg/"]

    def __init__(self):
        self.conn = sqlite3.connect('telegraph_data.db')
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS news
                            (url TEXT, title TEXT, article_time TEXT)''')
        self.conn.commit()

        schema_path = os.path.join(os.path.dirname(__file__), '..', 'schema.json')
        with open(schema_path) as f:
            self.schema = json.load(f)        

    def close(self):
        self.conn.close()

    def parse(self, response):
        try:
            news_page_link = response.xpath('/html/body/header/nav/div/ul/li[1]/a/@href').get()
            yield response.follow(news_page_link, callback=self.parse_news_link)
        except Exception as e:
            logging.error("Error parsing homepage: %s", str(e))

    def parse_news_link(self, response):
        try:
            news_page_urls = response.xpath('//h2[@class="second-title"]//a/@href').getall()
            for news_page_url in news_page_urls:
                yield response.follow(news_page_url, callback=self.parse_news_page)
        except Exception as e:
            logging.error("Error parsing news link: %s", str(e))

    def parse_news_page(self, response):
        try:
            news_url = response.url
            news_title = response.xpath('(//title)[1]//text()').get()
            news_time = response.xpath('//span[@class="article-time"]/text()').get()

            news_data = {
                'url': news_url,
                'title': news_title,
                'article_time': news_time
            }

            try:
                validate(instance=news_data, schema=self.schema)
            except ValidationError as e:
                logging.error("Validation error: %s", str(e))
                return
            
            self.cursor.execute("SELECT * FROM news WHERE url=?", (news_url,))
            existing_news = self.cursor.fetchone()

            if not existing_news:
                try:
                    self.cursor.execute("INSERT INTO news (url, title, article_time) VALUES (?,?,?)",
                                        (news_url,
                                        news_title,
                                        news_time.strip() if news_time else None))
                    self.conn.commit()
                    logging.info("Data inserted into SQLite successfully.")
                except sqlite3.Error as e:
                    logging.error("Error inserting data into SQLite: %s", str(e))
        except Exception as e:
            logging.error("Error parsing news element: %s", str(e))

        yield news_data
