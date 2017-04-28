from scrapy.crawler import CrawlerProcess
from http_handler import http_service
# from vn_express.spider import VNExpressNewspaperSpider
# from tuoi_tre.spider import TuoiTreNewspaperSpider
# from dan_tri.spider import DanTriNewspaperSpider
# from us.usa_today.spider import UsaTodaySpider
from us.daily_news.spider import DailyNewsSpider
from us.losangeles_times.spider import LosAngelesTimesSpider
# from us.new_york_times.spider import TheNewYorkTimesSpider
# from us.washington_post.spider import WashingtonPost
from us.newyork_post.spider import NewYorkPostSpider

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

start_urls = ['http://nypost.com/']
NewYorkPostSpider.setup(start_urls, 'new_york_post')

process.crawl(NewYorkPostSpider)
process.start()  # the script will block here until the crawling is finished
