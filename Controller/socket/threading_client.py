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
        self.waiting_url_list = None
        self.backup_node = None

    def run(self):
        print '[', threading.currentThread().getName(), '].... from, ', self.client_address
        try:

            while True:
                data = ''
                data_unit = self.connection.recv(4096)
                data += data_unit
                print '[_Line 27_:RECEIVED__FROM__', self.name, '___]', data_unit
                if len(data_unit) >= 4096:
                    while True:
                        data_unit = self.connection.recv(4096)
                        print 'data receive: ', data_unit
                        data += data_unit
                        if len(data_unit) < 4096:
                            break
                print '[_Line 36_:RECEIVED__FROM__', self.name, '___]',data
                if data:
                    self.handle(data)
                else:
                    self.interupt_handle()
                    self.cleanup()
        except socket.error:
            self.interupt_handle()
            self.cleanup()
        finally:
            self.connection.close()

    def interupt_handle(self):
        database_service = Database_Service()
        worker = database_service.get_worker_by_ip(self.client_address[0], self.client_address[1])
        database_service.block_change(self.name)
        backup_node = database_service.get_first_backup_node()
        if backup_node is not None:
            database_service.change_backup_to_worker(backup_node)
            crawled_url_list_obj = worker.crawled_url_list
            waiting_url_list_obj = worker.waiting_url_list
            crawled_url_list = []
            waiting_url_list = []
            # change worker contain crawled_url_list to backup_node
            for crawled_url in crawled_url_list_obj:
                database_service.delete_crawled_url(crawled_url)
                crawled_url_list.append(crawled_url.url)
            # change worker contain waiting_url_list to backup_node
            for waiting_url in waiting_url_list_obj:
                database_service.delete_waiting_url(waiting_url)
                waiting_url_list.append(waiting_url.url)

            backup_thread = None
            for t in threading.enumerate():
                if t.getName() == backup_node.thread_name:
                    backup_thread = t
                    break

            backup_thread.send_backup_crawled_url(crawled_url_list)
            self.waiting_url_list = waiting_url_list
            self.backup_node = backup_node
        else:
            print 'we have no backup node....'

    def send_backup_waiting_url(self, waiting_url_list):
        database_service = Database_Service()
        base_urls = []
        urls = {}
        group_id = database_service.get_groud_id()
        for url in waiting_url_list:
            temp = url.split('/')
            base_url = temp[0] + '//' + temp[2]
            site_crawl = database_service.get_site_crawl_with_base_url(base_url)
            database_service.add_waiting_url(site_crawl, self.worker, url, group_id + 1)
            if base_url not in base_urls:
                base_urls.append(base_url)
                urls[base_url] = []
            urls[base_url].append(url)
        data = {
            'type': 'crawl',
            'group_id': group_id + 1,
            'urls': urls
        }
        self.send_data(json.dumps(data))

    def send_backup_crawled_url(self, crawled_url_list):
        database_service = Database_Service()
        base_urls = []
        urls = {}
        group_id = database_service.get_groud_id()
        for url in crawled_url_list:
            temp = url.split('/')
            base_url = temp[0] + '//' + temp[2]
            if base_url not in base_urls:
                base_urls.append(base_url)
                urls[base_url] = []
            urls[base_url].append(url)
        data = {
            'type': 'backup_crawl',
            'group_id': group_id + 1,
            'urls': urls
        }
        self.send_data(json.dumps(data))

    def handle(self, raw_data):
        database_service = Database_Service()
        data = json.loads(raw_data)
        print '[_Line 123_:RECEIVED__FROM__', self.name, '___]', data
        if data['type'] == 'crawled':
            group_id = data['group_id']
            url_dict = data['urls']
            database_service.block_change(self.name)
            # check next url list in database, if less than 50, then send back to client all next url to crawl
            next_url_list = database_service.get_next_url_list()
            length_next_url_list = len(next_url_list)
            len_before_add = length_next_url_list
            for base_url in url_dict.keys():
                database_service.add_next_url(base_url, url_dict[base_url])
            if (length_next_url_list >= 10) and (length_next_url_list <= 50):
                data = self.convert_to_json(next_url_list,self.name)
                self.send_data(data)
            elif length_next_url_list > 50:
                self.handle_url_request(next_url_list)

            wait_url_list = database_service.get_wait_url_list_with_group_id(group_id)
            for wait_url in wait_url_list:
                database_service.add_crawled_url(wait_url.base_url,
                                                 wait_url.url,
                                                 worker_node=self.worker)
            if len_before_add < 10:
                next_list_obj = database_service.get_next_url_list()
                next_list = []
                for next_url_obj in next_list_obj:
                    next_list.append(next_url_obj)
                if (len(next_list) >= 10) and (len(next_list) <= 50):
                    data = self.convert_to_json(next_list,self.name)
                    self.send_data(data)
                elif len(next_list) > 50:
                    self.handle_url_request(next_list)

        elif data['type'] == 'join':
            if data['data'] == 'worker':
                self.worker = Worker_Node(ip=self.client_address[0],
                                          port=self.client_address[1],
                                          status='enable',
                                          thread_name=threading.currentThread().getName(),
                                          type='worker')
            else:
                self.worker = Worker_Node(ip=self.client_address[0],
                                          port=self.client_address[1],
                                          status='enable',
                                          thread_name=threading.currentThread().getName(),
                                          type='backup')
            database_service.add_woker(self.worker)
            print '[', threading.currentThread().getName(), '].... create worker obj for address ip: %s port %s' % \
                                                            (self.client_address[0], self.client_address[1])
        elif data['type'] == 'overload':
            group_id = data['group_id']
            url_dict = data['urls']
            for base_url in url_dict.keys():
                database_service.add_next_url(base_url, url_dict[base_url])
            wait_url_list = database_service.get_wait_url_list_with_group_id(group_id)
            for wait_url in wait_url_list:
                database_service.add_crawled_url(wait_url.base_url, wait_url.url, self.worker)
                #   change status for this worker to overload.
            database_service.change_worker_to_overload(self.worker)
            #     nothing to do with this worker,but we still keep this connect to worker for retriev crawled data
            backup_node = database_service.get_first_backup_node()
            if backup_node is not None:
                database_service.change_backup_to_worker(backup_node)
                database_service.block_change(backup_node.thread_name)
                next_url_list = database_service.get_next_url_list()[:5]
                thread = None
                for t in threading.enumerate():
                    if t.getName()== backup_node.thread_name:
                        thread = t
                        break
                data = self.convert_to_json(next_url_list,backup_node.thread_name)
                thread.send_data(data)
            else:
                print 'we have no backup node....'

        elif data['type'] == 'backup':
            url_dict = data['urls']
            database_service.block_change(self.name)
            for base_url in url_dict.keys():
                site_crawl = database_service.get_site_crawl_with_base_url(base_url)
                for url in url_dict[base_url]:
                    database_service.add_crawled_url(site_crawl, url, self.worker)
            # after save crawled url into database, send backup waiting url like normal crawl action.
            waiting_url_list = None
            for t in threading.enumerate():
                if t.get_backup_node.thread_name == self.worker.thread_name:
                    waiting_url_list = t.get_waiting_list
            self.send_backup_waiting_url(waiting_url_list)

    def get_backup_node(self):
        return self.backup_node

    def get_waiting_list(self):
        return self.waiting_url_list

    def handle_url_request(self, next_url_list):
        database_service = Database_Service()
        length_next_url_list = len(next_url_list)
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
            data = self.convert_to_json(list_to_send,worker.thread_name)
            thread.send_data(data)
            count = count + length_unit + 1

    def send_data(self, data):
        database_service = Database_Service()
        print '[_Line_244_:SEND_TO__'+self.name+']' + data
        self.connection.sendall(data)
        database_service.block_change(self.name)

    def check_live(self):
        data = {
            'type': 'check_live',
        }
        self.send_data(json.dumps(data))

    def convert_to_json(self, url_list, thread_name):
        database_service = Database_Service()
        base_urls = []
        urls = {}
        group_id = database_service.get_groud_id()
        for url_obj in url_list:
            if url_obj is not None:
                site_crawl = url_obj.base_url
                database_service.add_waiting_url(site_crawl, thread_name, url_obj.url, group_id + 1)
                base_url = site_crawl.base_url
                if base_url not in base_urls:
                    base_urls.append(base_url)
                    urls[base_url] = []
                urls[base_url].append(url_obj.url)

        data = {
            'type': 'crawl',
            'group_id': group_id + 1,
            'urls': urls
        }
        return json.dumps(data)

    def cleanup(self):
        self.connection.close()
        # threading.currentThread().stop()
        # threading.currentThread().join()
