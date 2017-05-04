from Server_side.database.database import Site_Crawl, Worker_Node, Database_Service, Waiting_Url_List
from Server_side.socket.socket_client import Client_Service

import json


class Server_service:
    def __init__(self):
        self.group_id = 0

    def get_base_url_request(self, base_url_list):
        database_service = Database_Service()
        for base_url in base_url_list:
            site_crawl = Site_Crawl(base_url=base_url)
            database_service.add_site_crawl(site_crawl)
        # get list worker is working
        worker_list = database_service.get_list_worker_exist()
        exist_worker_service_list = []
        for worker in worker_list:
            worker_service = Client_Service((worker.ip, int(worker.port)))
            is_exist = worker_service.is_live()
            if is_exist:
                exist_worker_service_list.append(worker_service)
        self.request_crawl_base_url(base_url_list, exist_worker_service_list)

    def request_crawl_base_url(self, next_url_list, exist_worker_service_list):
        database_service = Database_Service()
        # caculate unit lenght for each worker that exist
        worker_list_lenght = len(exist_worker_service_list)
        lenght_unit = len(next_url_list) / worker_list_lenght
        last_unit_list_lenght = len(next_url_list) % worker_list_lenght
        count = 0

        for worker_service in exist_worker_service_list:
            if count != len(next_url_list) - last_unit_list_lenght:
                list_to_send = next_url_list[count + 1:]
            else:
                list_to_send = next_url_list[count:(count + lenght_unit)]
            wait_url_obj_list = []
            for url in list_to_send:
                wait_url_obj = Waiting_Url_List(url=url, group_id=self.group_id)
                wait_url_obj_list.append(wait_url_obj)
                database_service.add_waiting_url(database_service.get_site_crawl_with_base_url(url), wait_url_obj)
            self.group_id += 1
            worker_service.request_next_url(wait_url_obj_list, group_id=self.group_id - 1)
            count = count + lenght_unit + 1




if __name__ == '__main__':
    # base_url_list = sys.argv
    server_service = Server_service()
    server_service.get_base_url_request(['http://abcnews.go.com', 'http://edition.cnn.com', ])
