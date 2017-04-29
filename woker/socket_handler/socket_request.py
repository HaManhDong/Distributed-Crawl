from scrapy.crawler import CrawlerProcess
from woker.us.abc_news.spider import ABCNewsSpider
from woker.us.cnn.spider import CNNSpider
from woker.us.daily_news.spider import DailyNewsSpider
from woker.us.chicago_suntimes.spider import ChicagoSuntimesSpider
import socket
import json

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

server_address = ('localhost', 11000)
print 'woker server starting up on %s port %s' % server_address
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)


def connect_server(ip, port):
    # Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (ip, port)
    sock.connect(server_address)
    return sock


def get_spider(base_url):
    return {
        'http://abcnews.go.com': ABCNewsSpider,
        'http://edition.cnn.com': CNNSpider,
        'http://www.nydailynews.com': DailyNewsSpider,
        'http://chicago.suntimes.com': ChicagoSuntimesSpider,
    }.get(base_url, None)


def message_handler(message):
    reply = {
        'type': 'crawled',
        'groupID': 0,
        'urls': {}
    }
    next_urls = []
    message = json.loads(message)
    if message['type'] == 'crawl':
        reply['groupID'] = message['groupID']
        if message['urls']:
            process = get_process_crawl()
            spider_list = []
            for key in message['urls']:
                reply['urls'][key] = []
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
            # for spider in spider_list:
            #     next_urls = next_urls + spider.get_next_urls()

            for spiderKey in reply['urls']:
                reply['urls'][key] = get_spider(key).get_next_urls()
            print len(next_urls)
            print reply
    return reply


def get_process_crawl():
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    return process

if __name__ == "__main__":
    sock = connect_server('localhost', 50000)
    message = {
        'type': 'join'
    }
    try:
        # Send data
        message = json.dumps(message)
        print 'sending ', message
        sock.sendall(message)

        data = sock.recv(1024)
        reply = message_handler(data)
        reply = json.dumps(reply)
        sock.sendall(reply)

    finally:
        pass
        # print 'closing socket'
        # sock.close()

