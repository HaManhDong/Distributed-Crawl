import datetime
from woker.database.database_connection import NewsData
import scrapy
from woker.database import database_connection


class CNNSpider(scrapy.Spider):
    name = "cnn"
    db_session = database_connection.create_database_and_connect(name)
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
            CNNSpider.db_session.add(news_page_data)
            CNNSpider.db_session.commit()

        CNNSpider.next_urls += self.get_next_link_list(response)

        # write to file
        target = open(self.name, 'w')
        for url in CNNSpider.next_urls:
            target.write(url)
            target.write('\n')
        target.close()

    @staticmethod
    def get_next_link_list(response):
        nex_link_list = []
        href_element = response.xpath("//a[@href]")
        for link in href_element:
            link_url = link.xpath("./@href").extract_first()
            if 'http://edition.cnn.com' not in link_url and "cnn"not in link_url:
                link_url = 'http://edition.cnn.com' + link_url
                nex_link_list.append(link_url)
        CNNSpider.next_urls += nex_link_list
        return nex_link_list

    @staticmethod
    def get_title(response):
        news_title_element = response.xpath('/html/head//title/text()')
        if len(news_title_element) > 0:
            return news_title_element.extract_first()
        return None

    @staticmethod
    def get_content(response):
        content_block_element = response.xpath(
            '//div[@itemprop="articleBody"]')
        if len(content_block_element) > 0:
            return_text = ''
            paragraph_nodes = content_block_element[0].xpath('.//p[@class="zn-body__paragraph"]')
            div_nodes = content_block_element[0].xpath('.//div[@class="zn-body__paragraph"]')
            for node in div_nodes:
                paragraph_nodes.append(node)
            for paragraph_node in paragraph_nodes:
                text_nodes = paragraph_node.xpath(".//text()")
                for text_node in text_nodes:
                    return_text += text_node.extract()
            return return_text
        return None

    @staticmethod
    def get_time(response):
        datetime_element = response.xpath('/html/head//meta[@name="pubdate"]/@content')
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
    def get_type(response):
        type_element = response.xpath('/html/head//meta[@name="section"]/@content')
        if len(type_element) > 0:
            try:
                type_data = type_element.extract_first().split(',')[0]
                return type_data
            except Exception:
                return None
        return None
