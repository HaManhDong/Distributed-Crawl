from scrapy.crawler import CrawlerProcess
from us.abc_news.spider import ABCNewsSpider
from us.cnn.spider import CNNSpider
from us.daily_news.spider import DailyNewsSpider
import socket
import json

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_address = ('localhost', 11000)
print 'woker server starting up on %s port %s' % server_address
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)


def get_spider(base_url):
    return {
        'http://abcnews.go.com': ABCNewsSpider,
        'http://edition.cnn.com': CNNSpider,
        'http://www.nydailynews.com': DailyNewsSpider,
    }.get(base_url, None)


def message_handler(connection, message):
    message = json.loads(message)
    if message['type'] == 'check_live':
        connection.send('ok')
        return
    if message['type'] == 'crawl':
        if (message['urls']):
            process = get_process_crawl()
            spider_list = []
            for key in message['urls']:
                print "key: %s , value: %s" % (key, message['urls'][key])
                spider = get_spider(key)
                if spider != None:
                    # action_crawl(spider, message['urls'][key])
                    db_name = key.split('//')[1]
                    if db_name.split('.')[0] != 'www':
                        db_name = db_name.split('.')[0]
                    else:
                        db_name = db_name.split('.')[1]
                    spider.setup(message['urls'][key], db_name)
                    spider_list.append(spider)
                    process.crawl(spider)
            process.start() # the script will block here until the crawling is finished
            next_urls = []
            for spider in spider_list:
                next_urls = next_urls + spider.get_next_urls()
            print len(next_urls)
            print next_urls


def get_process_crawl():
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    return process

while True:
    # Wait for a connection
    print 'waiting for a connection'
    connection, client_address = sock.accept()
    data = ''
    try:
        print 'connection from', client_address

        # Receive the data in small chunks and retransmit it
        data_unit = connection.recv(4096)
        print 'received "%s"' % data_unit
        # connection.sendall(data)
        message_handler(connection, data_unit)

    finally:
        # Clean up the connection
        connection.close()
