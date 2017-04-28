from database.database_connection import NewsData
import scrapy
from scrapy.exceptions import CloseSpider
from database import database_connection


# Abstract class for distributed crawler
class DistributedSpider(scrapy.Spider):
    start_urls = []
    db_session = ''
    url_set = {}
    n_id = 0

    @staticmethod
    def setup(start_urls, db_name):
        DistributedSpider.db_session = database_connection.create_database_and_connect(db_name)
        DistributedSpider.start_urls = start_urls

    def parse(self, response):
        news_title = self.get_title(response)
        news_time = self.get_time(response)
        content = self.get_content(response)
        news_type = self.get_type(response)
        url = response.url
        # check if this page is crawled
        if not DistributedSpider.check_if_page_is_crawled(url):
            if news_title is not None and content is not None and news_time is not None and news_type is not None:
                news_page_data = NewsData(url=url, title=news_title, content=content,
                                          time=news_time, type=news_type)
                DistributedSpider.db_session.add(news_page_data)
            DistributedSpider.db_session.commit()
            crawled_page = len(DistributedSpider.url_set)
            DistributedSpider.url_set[url] = DistributedSpider.n_id
            DistributedSpider.n_id += 1
            # check is crawled page length is reach limit
            if crawled_page > 200000:
            # if crawled_page > 5:
                raise CloseSpider('Search Exceeded 200000')
        next_link_list = self.get_next_link_list(response)
        next_urls = []
        for link_url in next_link_list:
            if not DistributedSpider.check_if_page_is_crawled(link_url):
                next_urls.append(link_url)
        print "******* next link ******"
        print len(DistributedSpider.url_set)
        print len(next_link_list)
        # print len(DistributedSpider.next_urls)
        # for next_link in next_urls:
        #     yield scrapy.Request(next_link, callback=self.parse)

    @staticmethod
    def check_if_page_is_crawled(check_url):
        if check_url in DistributedSpider.url_set:
            return True
        return False

    @staticmethod
    def get_title(response):
        pass

    @staticmethod
    def get_time(response):
        pass

    @staticmethod
    def get_content(response):
        pass

    @staticmethod
    def get_type(response):
        pass

    @staticmethod
    def get_next_link_list(response):
        return []
