from scrapy.crawler import CrawlerProcess
from us.huffington_post.spider import HuffingtonPostSpider

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

start_urls = ['http://www.huffingtonpost.com']
HuffingtonPostSpider.setup(start_urls, 'HuffingtonPost')

process.crawl(HuffingtonPostSpider)
process.start()  # the script will block here until the crawling is finished
