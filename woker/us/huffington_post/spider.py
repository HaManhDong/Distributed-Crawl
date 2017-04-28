import datetime
from distributed_crawler.distributed_crawler import DistributedSpider


class HuffingtonPostSpider(DistributedSpider):
    name = "Huffington_Post"

    @staticmethod
    def get_next_link_list(response):
        nex_link_list = []
        href_element = response.xpath("//a[contains(@href,'http://www.huffingtonpost.com')]")
        for link in href_element:
            link_url = link.xpath("./@href").extract_first()
            nex_link_list.append(link_url)
        return nex_link_list

    @staticmethod
    def get_title(response):
        news_title_element = response.xpath('//h1[@class="headline__title"]/text()')
        if len(news_title_element) > 0:
            return news_title_element.extract_first()
        return None


    @staticmethod
    def get_content(response):
        content_block_element = response.xpath(
            '//div[@class="entry__container"]/div[contains(@class,"entry__content")]')
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
        datetime_element = response.xpath('/html/head//meta[@property="article:published_time"]/@content')
        if len(datetime_element) > 0:
            try:
                date_time = datetime_element.extract_first().split(" ")
                datetime_data = date_time[0]+"T"+date_time[1]
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
        type_element = response.xpath('/html/head//meta[@property="article:section"]/@content')
        if len(type_element) > 0:
            try:
                type_data = type_element.extract_first().split(',')[0]
                return type_data
            except Exception:
                return None
        return None
