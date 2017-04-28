import datetime
from database.database_connection import NewsData
import scrapy
from database import database_connection


class NewsItem:
    def __init__(self, url, title, summary, content):
        self.url = url
        self.title = title
        self.summary = summary
        self.content = content


class DailyNewsSpider(scrapy.Spider):
    name = "Daily_News"
    db_session = ''
    start_urls = []
    next_urls = []

    @staticmethod
    def setup(start_urls, db_name):
        DailyNewsSpider.db_session = database_connection.create_database_and_connect(db_name)
        DailyNewsSpider.start_urls = start_urls

    def parse(self, response):
        news_title = self.get_title(response)
        news_time = self.get_time(response)
        content = self.get_content(response)
        news_type = self.get_type(response)
        url = response.url

        if news_title is not None and content is not None and news_time is not None and news_type is not None:
            news_page_data = NewsData(url=url, title=news_title, content=content,
                                      time=news_time, type=news_type)
            DailyNewsSpider.db_session.add(news_page_data)
            DailyNewsSpider.db_session.commit()

        DailyNewsSpider.next_urls += self.get_next_link_list(response)

    @staticmethod
    def get_title(response):
        news_title_element = response.xpath('//h1[@itemprop="headline"]/text()')
        if len(news_title_element) > 0:
            return news_title_element.extract_first()
        return None

    @staticmethod
    def get_content(response):
        content_block_element = response.xpath(
            '//article[contains(@itemprop,"articleBody")]')
        # if len(content_block_element) <= 0:
        #     content_block_element = response.xpath('//*[contains(@class,"block_content_slide_showdetail")]')
        if len(content_block_element) > 0:
            return_text = ''
            paragraph_nodes = content_block_element[0].xpath(".//p")
            for paragraph_node in paragraph_nodes:
                text_nodes = paragraph_node.xpath(".//text()")
                for text_node in text_nodes:
                    return_text += text_node.extract()
            return return_text
        return None

    @staticmethod
    def get_time(response):
        # content_block_element =
        # response.xpath("//div[contains(@class, 'block_timer_share') and contains(@class, 'class2')]")
        datetime_element = response.xpath('/html/head//meta[@name="parsely-pub-date"]/@content')
        if len(datetime_element) > 0:
            try:
                datetime_data = datetime_element.extract_first()[0:19]
                try:
                    checkDate = datetime.datetime.strptime(datetime_data, "%Y-%m-%dT%H:%M:%S")
                    return datetime_data
                except ValueError:
                    return None
            except Exception:
                return None
        return None

    @staticmethod
    def get_next_link_list(response):
        next_link_list = []
        href_element = response.xpath("//a[starts-with(@href, 'http://www.nydailynews.com')]")
        for link in href_element:
            link_url = link.xpath("./@href").extract_first()
            next_link_list.append(link_url)
        return next_link_list

    @staticmethod
    def get_type(response):
        type_element = response.xpath('//body/@data-section')
        if len(type_element) > 0:
            try:
                type_data = type_element.extract_first()
                return type_data
            except Exception:
                return None
        return None

    @staticmethod
    def get_next_urls():
        return DailyNewsSpider.next_urls
