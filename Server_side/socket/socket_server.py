from Server_side.socket.threading_client import Thread_Socket_Client
from Server_side.database.database import Database_Service, Site_Crawl
import socket
import time

SERVER_ADDRESS = ('localhost', 10005)


class Server:
    threads = []

    def __init__(self, server_address, base_list):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(server_address)
        self.server_socket.listen(5)
        self.counter = 0
        self.base_list = base_list
        print 'starting at ', server_address[1], '....'

    def run(self):
        database_service = Database_Service()
        while True:
            print 'waiting for a connection....'
            worker_list_exist = database_service.get_list_worker_exist()
            if len(worker_list_exist) == 3:
                self.start_crawl(self.base_list, worker_list_exist)
            connection, client_address = self.server_socket.accept()
            name = 'client_' + str(self.counter)
            new_thread = Thread_Socket_Client(connection, client_address, name=name)
            new_thread.start()
            self.threads.append(new_thread)
            self.counter += 1
            time.sleep(2)

    def start_crawl(self, base_url_list, worker_list_exist):
        database_service = Database_Service()
        for base_url in base_url_list:
            site_crawl = Site_Crawl(base_url=base_url)
            database_service.add_site_crawl(site_crawl=site_crawl)
            database_service.add_next_url(site_crawl=site_crawl, url_list=[base_url, ])
        # calculate unit length for each worker that exist
        worker_list_length = len(self.threads)
        length_unit = len(base_url_list) / worker_list_length
        last_unit_list_length = len(base_url_list) % worker_list_length
        count = 0

        for worker in worker_list_exist:
            if count == len(base_url_list) - last_unit_list_length:
                list_to_send = base_url_list[count + 1:]
            else:
                list_to_send = base_url_list[count:(count + length_unit)]
            thread = None
            for t in self.threads:
                if t.getName() == worker.thread_name:
                    thread = t
            data = thread.convert_to_json(list_to_send)
            thread.send_data(data)
            count = count + length_unit + 1


if __name__ == '__main__':
    url = ['a.com', 'b.com', 'c.com', 'd.com']
    server = Server(SERVER_ADDRESS, base_list=url)
    server.run()