import datetime
from distributed_crawler.distributed_crawler import DistributedSpider


class ChicagoSuntimesSpider(DistributedSpider):
    name = "Chicago_Suntimes"

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
