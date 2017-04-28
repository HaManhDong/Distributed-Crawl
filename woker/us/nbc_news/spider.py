import datetime
from distributed_crawler.distributed_crawler import DistributedSpider


class NBCNewsSpider(DistributedSpider):
    name = "NBC News"

    @staticmethod
    def get_next_link_list(response):
        nex_link_list = []
        href_element = response.xpath("//a[@href]")
        for link in href_element:
            link_url = link.xpath("./@href").extract_first()
            if 'http://www.nbcnews.com' not in link_url and "nbc"not in link_url and 'http' not in link_url:
                link_url = 'http://www.nbcnews.com' + link_url
                nex_link_list.append(link_url)
            nex_link_list.append(link_url)
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
            '//time[@itemprop="dateModified"]/@datetime')
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
        type_element = response.xpath('//div[@itemprop="articleSection"]//a/text()')
        if len(type_element) > 0:
            try:
                type_data = type_element.extract_first().split(',')[0]
                return type_data
            except Exception:
                return None
        return None
