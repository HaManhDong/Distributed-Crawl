from scrapy.crawler import CrawlerProcess
from us.abc_news.spider import ABCNewsSpider
from us.cnn.spider import CNNSpider

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})
start_urls = ['http://abcnews.go.com']
start_urls2 = ['http://edition.cnn.com']

ABCNewsSpider.setup(start_urls, 'abc_news')
CNNSpider.setup(start_urls2, 'cnn_news')

process.crawl(ABCNewsSpider)
process.crawl(CNNSpider)
# process.crawl(TuoiTreNewspaperSpider)
# process.crawl(DanTriNewspaperSpider)
process.start()  # the script will block here until the crawling is finished

dong = ABCNewsSpider.get_next_urls()
print 'line 22:'
print len(dong)
print dong
dong += CNNSpider.get_next_urls()
print 'line 25:'
print len(dong)
print dong
