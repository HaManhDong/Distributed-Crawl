from scrapy.crawler import CrawlerProcess
from us.nbc_news.spider import NBCNewsSpider

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
start_urls = ['http://www.nbcnews.com']

NBCNewsSpider.setup(start_urls, 'nbc_news')

process.crawl(NBCNewsSpider)
# process.crawl(TuoiTreNewspaperSpider)
# process.crawl(DanTriNewspaperSpider)
process.start()  # the script will block here until the crawling is finished
