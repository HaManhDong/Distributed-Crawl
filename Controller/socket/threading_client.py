from Controller.database.database import Worker_Node, Database_Service, Waiting_Url_List
import threading
import socket
import json


class Thread_Socket_Client(threading.Thread):
    def __init__(self, connection, client_address,
                 group=None, target=None, name=None, args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        self.connection = connection
        self.client_address = client_address
        self.worker = None
        self.name = name

    def run(self):
        print '[', threading.currentThread().getName(), '].... from, ', self.client_address
        try:

            while True:
                data = ''
                # TODO: add code revecive enought data
                while True:
                    data_unit = self.connection.recv(4096)
                    print 'data receive: ', data_unit
                    data += data_unit
                    if len(data_unit) < 4096:
                        break
                print "line 32:"
                print data
                if data:
                    self.handle(data)
        except socket.error, msg:
            print 'error ', msg, ' from ', self.client_address
        finally:
            self.connection.close()

    def handle(self, raw_data):
        database_service = Database_Service()
        data = json.loads(raw_data)
        print 'line 43: ', data
        if data['type'] == 'crawled':
            group_id = data['group_id']
            url_dict = data['urls']
            database_service.block_change(self.name)
            # check next url list in database, if less than 50, then send back to client all next url to crawl
            next_url_list = database_service.get_next_url_list()
            length_next_url_list = len(next_url_list)
            len_before_add = length_next_url_list
            if (length_next_url_list >= 10) and (length_next_url_list <= 50):
                data = self.convert_to_json(next_url_list)
                self.send_data(data)
            elif length_next_url_list > 50:
                self.handle_url_request(next_url_list)

            for base_url in url_dict.keys():
                database_service.add_next_url(base_url, url_dict[base_url])
            wait_url_list = database_service.get_wait_url_list_with_group_id(group_id)
            for wait_url in wait_url_list:
                database_service.add_crawled_url(wait_url.base_url, wait_url.url)
            if len_before_add < 10:
                next_list_obj = database_service.get_next_url_list()
                next_list = []
                for next_url_obj in next_list_obj:
                    next_list.append(next_url_obj.url)
                if (len(next_list) >= 10) and (len(next_list) <= 50):
                    data = self.convert_to_json(next_list)
                    self.send_data(data)
                elif len(next_list) > 50:
                    self.handle_url_request(next_list)

        elif data['type'] == 'join':
            self.worker = Worker_Node(ip=self.client_address[0],
                                      port=self.client_address[1],
                                      status='enable',
                                      thread_name=threading.currentThread().getName())
            database_service.add_woker(self.worker)
            print '[', threading.currentThread().getName(), '].... create worker obj for address ip: %s port %s' % \
                                                            (self.client_address[0], self.client_address[1])

    def handle_url_request(self, next_url_list):
        length_next_url_list = len(next_url_list)
        database_service = Database_Service()
        worker_list_exist = database_service.get_list_worker_exist()
        length_worker_list = len(worker_list_exist)
        length_unit = length_next_url_list / length_worker_list
        if length_unit > 5:
            length_unit = 5
        remain_length = length_next_url_list % length_worker_list
        count = 0
        for worker in worker_list_exist:
            if count == length_next_url_list - remain_length:
                list_to_send = next_url_list[count + 1:]
            else:
                list_to_send = next_url_list[count:(count + length_unit)]
            thread = None
            for t in threading.enumerate():
                if t.getName() == worker.thread_name:
                    thread = t
                    break
            data = thread.convert_to_json(list_to_send)
            thread.send_data(data)
            count = count + length_unit + 1

    def send_data(self, data):
        self.connection.sendall(data)
        database_service = Database_Service()
        database_service.block_change(self.name)

    def check_live(self):
        data = {
            'type': 'check_live',
        }
        self.send_data(json.dumps(data))

    def convert_to_json(self, url_list):
        base_urls = []
        urls = {}
        database_service = Database_Service()
        group_id = database_service.get_groud_id()
        for url in url_list:
            url_obj = database_service.get_next_url_by_url(url)
            site_crawl = url_obj.base_url
            wait_obj = Waiting_Url_List(url=url, group_id=group_id + 1)
            database_service.add_waiting_url(site_crawl, wait_obj)
            base_url = site_crawl.base_url
            if base_url not in base_urls:
                base_urls.append(base_url)
                urls[base_url] = []
            urls[base_url].append(url)

        data = {
            'type': 'crawl',
            'group_id': group_id + 1,
            'urls': urls
        }
        return json.dumps(data)

    def cleanup(self):
        threading.currentThread().stop()
        threading.currentThread().join()
