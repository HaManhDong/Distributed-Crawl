from scrapy.crawler import CrawlerProcess
from woker.us.abc_news.spider import ABCNewsSpider
from woker.us.cnn.spider import CNNSpider
from woker.us.daily_news.spider import DailyNewsSpider
from woker.us.chicago_suntimes.spider import ChicagoSuntimesSpider
import socket
import json
from subprocess import call
import threading, time, os


def foo(spider_name, url):
    # time.sleep(4)
    command = 'scrapy crawl ' + spider_name + ' -a url="' + url + '"'
    # command = 'scrapy crawl abcnews -a url="http://abcnews.go.com/Politics/trump-asked-nsa-director-publicly-push-back-fbis/story?id=47578374&cid=clicksource_77_null_headlines_hed"'
    os.system(command)
    for t in threading.enumerate():
        if t.getName() == 'MainThread':
            count_url = t.__getattribute__('count_url')
            count_url -= 1
            t.__setattr__('count_url', count_url)
            break


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
        'http://abcnews.go.com': 'abcnews',
        'http://edition.cnn.com': CNNSpider,
        'http://www.nydailynews.com': DailyNewsSpider,
        'http://chicago.suntimes.com': 'chicago',
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
            count_url = 0

            for key in message['urls']:
                count_url += len(message['urls'][key])

            threading.currentThread().__setattr__('count_url', count_url)

            for key in message['urls']:
                reply['urls'][key] = []
                spider_name = get_spider(key)
                for url in message['urls'][key]:
                    t = threading.Thread(target=foo, args=(spider_name, url))
                    t.start()

                # time.sleep(6)

            while True:
                if threading.currentThread().__getattribute__('count_url') == 0:
                    for spiderKey in reply['urls']:
                        spider_name = get_spider(spiderKey)
                        with open(spider_name) as fp:
                            for line in fp:
                                if line:
                                    reply['urls'][spiderKey].append(line)
                                    # print line
                    break

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
            print "receive", data
            reply = message_handler(data)
            reply = json.dumps(reply)
            print "sending...", reply
            sock.sendall(reply)

    finally:
        print "connection was close"
        pass
        # print 'closing socket'
        # sock.close()
