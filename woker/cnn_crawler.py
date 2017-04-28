from scrapy.crawler import CrawlerProcess
from us.cnn.spider import CNNSpider

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
start_urls = ['http://edition.cnn.com/2016/11/17/football/steve-nash-owen-hargreaves-nba-basketball-football/index.html']

CNNSpider.setup(start_urls, 'cnn_news')

process.crawl(CNNSpider)
# process.crawl(TuoiTreNewspaperSpider)
# process.crawl(DanTriNewspaperSpider)
process.start()  # the script will block here until the crawling is finished
