from scrapy.crawler import CrawlerProcess
from woker.us.abc_news.spider import ABCNewsSpider
from woker.us.cnn.spider import CNNSpider
from woker.us.daily_news.spider import DailyNewsSpider
from woker.us.chicago_suntimes.spider import ChicagoSuntimesSpider
import socket
import json
from subprocess import call
import threading, time


class myThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        call(['scrapy crawl abcnews'])

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

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
        'group_id': 0,
        'urls': {}
    }
    next_urls = []
    message = json.loads(message)
    if message['type'] == 'crawl':
        reply['group_id'] = message['group_id']
        if message['urls']:
            process = get_process_crawl()
            spider_list = []
            for key in message['urls']:
                reply['urls'][key] = []
                # print "key: %s , value: %s" % (key, message['urls'][key])
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
                    spider.run()
                    # thread1 = myThread()
                    # thread1.start()
                    # call(['ls', '-la'])

                    # time.sleep(4)

                    # process.crawl(spider)
            # process.start() # the script will block here until the crawling is finished
            # for spider in spider_list:
            #     next_urls = next_urls + spider.get_next_urls()

            # for spiderKey in reply['urls']:
            #     reply['urls'][spiderKey] = get_spider(spiderKey).get_next_urls()
            # print len(next_urls)

            for spiderKey in reply['urls']:
                with open('abc.txt') as fp:
                    for line in fp:
                        if line:
                            reply['urls'][spiderKey].append(line)
                            # print line

            print reply
    return reply


def get_process_crawl():
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

    return process

if __name__ == "__main__":
    sock = connect_server('localhost', 10000)
    message = {
        'type': 'join',
        'data': 'worker'
    }
    try:
        # Send data
        message = json.dumps(message)
        print 'sending ', message
        sock.sendall(message)

        while True:
            data = sock.recv(8096)
            print "receive" ,data
            reply = message_handler(data)
            reply = json.dumps(reply)
            print "sending...", reply
            sock.sendall(reply)

    finally:
        print "connection was close"
        pass
        # print 'closing socket'
        # sock.close()


