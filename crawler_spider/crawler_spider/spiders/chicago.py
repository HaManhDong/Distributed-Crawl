import datetime
from woker.database.database_connection import NewsData
import scrapy
from woker.database import database_connection


class ChicagoSuntimesSpider(scrapy.Spider):
    name = "chicago"
    db_session = database_connection.create_database_and_connect(name)
    start_urls = []
    next_urls = []

    def start_requests(self):
        start = getattr(self, 'url', None)
        print start
        yield scrapy.Request(start, self.parse)

    def parse(self, response):
        news_title = self.get_title(response)
        news_time = self.get_time(response)
        content = self.get_content(response)
        news_type = self.get_type(response)
        url = response.url

        if news_title is not None and content is not None and news_time is not None and news_type is not None:
            news_page_data = NewsData(url=url, title=news_title, content=content,
                                      time=news_time, type=news_type)
            ChicagoSuntimesSpider.db_session.add(news_page_data)
            ChicagoSuntimesSpider.db_session.commit()

        ChicagoSuntimesSpider.next_urls += self.get_next_link_list(response)

        # write to file
        target = open(self.name, 'w')
        for url in ChicagoSuntimesSpider.next_urls:
            target.write(url)
            target.write('\n')
        target.close()

    @staticmethod
    def get_next_link_list(response):
        nex_link_list = []
        href_element = response.xpath("//a[contains(@href,'http://chicago.suntimes.com')]")
        for link in href_element:
            link_url = link.xpath("./@href").extract_first()
            nex_link_list.append(link_url)
        return nex_link_list

    @staticmethod
    def get_title(response):
        news_title_element = response.xpath('/html/head//title/text()')
        if len(news_title_element) > 0:
            return news_title_element.extract_first().split("|")[0]
        return None

    @staticmethod
    def get_content(response):
        content_block_element = response.xpath(
            '//div[@itemprop="articleBody"]')
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
        datetime_element = response.xpath(
            '//span[@class="post-relative-date top-date"]/text()')
        if len(datetime_element) > 0:
            try:

                datetime_data = datetime_element.extract_first()[0:17]
                date_data = datetime_data.split(",")[0].split('/')
                time_data = datetime_data.split(",")[1][1:7].split(':')
                try:
                    datetime_data = date_data[2] + '-' + date_data[0] + '-' + datetime_data[1] + \
                                    'T' + time_data[0] + ':' + time_data[1] + ':00'
                    checkDate = datetime.datetime.strptime(datetime_data, "%Y-%m-%dT%H:%M:%S")
                    return datetime_data
                except ValueError:
                    return None
            except Exception:
                return None
        return None

    @staticmethod
    def get_type(response):
        type_element = response.xpath('//a[contains(@id,"newsfeed-logo")]/text()')
        if len(type_element) > 0:
            try:
                type_data = type_element.extract_first().split(',')[0]
                return type_data
            except Exception:
                return None
        return None

    @staticmethod
    def get_next_urls():
        return ChicagoSuntimesSpider.next_urls

