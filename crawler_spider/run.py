import os
import threading
from scrapy.crawler import Crawler
import time

def foo():
    # time.sleep(4)
    command = 'scrapy crawl abcnews -a url="http://abcnews.go.com/Politics/trump-asked-nsa-director-publicly-push-back-fbis/story?id=47578374&cid=clicksource_77_null_headlines_hed"'
    os.system(command)


t = threading.Thread(target=foo,name='test')
t.start()
print 'xxxxxx'
threading.currentThread().__setattr__('next_urls',None)