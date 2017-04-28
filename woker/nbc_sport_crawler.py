from scrapy.crawler import CrawlerProcess
from us.nbc_news_sport.spider import NBCSportSpider

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
start_urls = ['http://www.nbcsports.com/']

NBCSportSpider.setup(start_urls, 'nbc_sports')

process.crawl(NBCSportSpider)
# process.crawl(TuoiTreNewspaperSpider)
# process.crawl(DanTriNewspaperSpider)
process.start()  # the script will block here until the crawling is finished
