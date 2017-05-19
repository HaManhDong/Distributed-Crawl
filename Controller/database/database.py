from sqlalchemy import create_engine, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.orm import relationship
import os

Base = declarative_base()
FOLDER_PATH = os.path.dirname(os.path.abspath(__file__))
engine = create_engine('sqlite:///' + FOLDER_PATH + '/db.sqlite')


class Site_Crawl(Base):
    __tablename__ = 'site_crawl'
    id = Column(Integer, primary_key=True)
    base_url = Column(String)
    next_url = relationship('Next_Url_List', cascade="all, delete-orphan")
    crawled_url = relationship('Crawled_Url_List', cascade="all, delete-orphan")
    waiting_url = relationship('Waiting_Url_List', cascade="all, delete-orphan")


class Next_Url_List(Base):
    __tablename__ = 'next_url_list'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    base_url_id = Column(Integer, ForeignKey('site_crawl.id'))
    base_url = relationship('Site_Crawl', back_populates="next_url")

    def __repr__(self):
        return self.url


class Crawled_Url_List(Base):
    __tablename__ = 'crawled_url_list'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    base_url_id = Column(Integer, ForeignKey('site_crawl.id'))
    base_url = relationship('Site_Crawl', back_populates="crawled_url")
    worker_id = Column(Integer, ForeignKey('worker_node.id'))
    worker = relationship('Worker_Node', back_populates='crawled_url_list')

    def __repr__(self):
        return self.url


class Waiting_Url_List(Base):
    __tablename__ = 'waiting_url_list'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    group_id = Column(Integer)
    base_url_id = Column(Integer, ForeignKey('site_crawl.id'))
    base_url = relationship('Site_Crawl', back_populates="waiting_url")
    worker_id = Column(Integer, ForeignKey('worker_node.id'))
    worker = relationship('Worker_Node', back_populates='waiting_url_list')

    def __repr__(self):
        return self.url


class Worker_Node(Base):
    __tablename__ = 'worker_node'
    id = Column(Integer, primary_key=True)
    ip = Column(String)
    port = Column(String)
    status = Column(String)
    thread_name = Column(String)
    type = Column(String)
    crawled_url_list = relationship('Crawled_Url_List', cascade="all, delete-orphan")
    waiting_url_list = relationship('Waiting_Url_List', cascade="all, delete-orphan")


class Database_Service:
    def __init__(self):
        self.session = scoped_session(sessionmaker(bind=engine))

    def add_woker(self, worker):
        self.session.add(worker)
        self.session.commit()

    def block_change(self, thread_name):
        worker = self.session.query(Worker_Node).filter_by(thread_name=thread_name).first()
        if worker.status == 'enable':
            worker.status = 'disable'
        else:
            worker.status = 'enable'
        self.session.commit()

    def add_waiting_url(self, site_crawl, worker_node, url, group_id):
        next_url_obj = self.get_next_url_by_url(url)
        if next_url_obj is not None:
            self.session.delete(next_url_obj)
        waiting_url_obj = Waiting_Url_List(url=url, group_id=group_id)
        site_crawl.waiting_url.append(waiting_url_obj)
        worker_node.waiting_url_list.append(waiting_url_obj)
        self.session.commit()

    def remove_duplica(self, url_list):
        result = []
        for url in url_list:
            if url not in result:
                result.append(url)
        return result

    def add_next_url(self, site_crawl, url_list):
        site_crawl_obj = self.get_site_crawl_with_base_url(site_crawl)
        crawled_url_list = self.session.query(Crawled_Url_List).filter_by(base_url=site_crawl_obj).all()
        waiting_url_list = self.session.query(Waiting_Url_List).filter_by(base_url=site_crawl_obj).all()
        current_next_url_list = self.session.query(Next_Url_List).filter_by(base_url=site_crawl_obj).all()
        a = []
        url_list_after = self.remove_duplica(url_list)
        for url in url_list_after:
            check_exist = False
            for crawled_url in crawled_url_list:
                if url == crawled_url.url:
                    check_exist = True
                    break
            for next_url in current_next_url_list:
                if url == next_url.url:
                    check_exist = True
                    break

            for wait_url in waiting_url_list:
                if url == wait_url.url:
                    check_exist = True
                    break
            if check_exist == False:
                next_url = Next_Url_List(url=url)
                site_crawl_obj.next_url.append(next_url)
                a.append(url)
        self.session.commit()

    def add_crawled_url(self, site_crawl, url, worker_node):
        wait_url = self.session.query(Waiting_Url_List).filter_by(url=url).first()
        if wait_url is not None:
            self.session.delete(wait_url)
        crawled_url = Crawled_Url_List(url=url)
        site_crawl.crawled_url.append(crawled_url)
        worker_node.crawled_url_list.append(crawled_url)
        self.session.commit()

    def get_worker_to_crawl_base_url(self):
        worker = self.session.query(Worker_Node).first()
        return worker

    def add_site_crawl(self, site_crawl):
        self.session.add(site_crawl)
        self.session.commit()

    def get_list_worker_exist(self):
        worker_list = self.session.query(Worker_Node).filter_by(type='worker').filter_by(status='enable').all()
        return worker_list

    def get_site_crawl_with_base_url(self, base_url):
        site_crawl = self.session.query(Site_Crawl).filter_by(base_url=base_url).first()
        return site_crawl

    def get_wait_url_list_with_group_id(self, group_id):
        wait_url_list = self.session.query(Waiting_Url_List).filter_by(group_id=group_id).all()
        return wait_url_list

    def get_next_url_list(self):
        next_url_list = self.session.query(Next_Url_List).all()
        return next_url_list

    def get_next_url_by_url(self, url):
        next_url_obj = self.session.query(Next_Url_List).filter_by(url=url).first()
        return next_url_obj

    def get_groud_id(self):
        rsp = self.session.query(func.max(Waiting_Url_List.group_id).label('max_groud_id')).first()
        url_obj = rsp.max_groud_id
        if url_obj is None:
            return 0
        else:
            return url_obj

    def get_worker_by_ip(self, ip, port):
        worker_node = self.session.query(Worker_Node).filter_by(ip=ip, port=port).first()
        return worker_node

    def get_first_backup_node(self):
        backup_node = self.session.query(Worker_Node).filter_by(type='backup').first()
        return backup_node

    def change_backup_to_worker(self, backup_node):
        backup_node.type = 'worker'
        backup_node.status = 'disable'
        self.session.commit()
        return True

    def change_worker_to_overload(self, worker_node):
        worker_node.status = 'overload'
        self.session.commit()
        return True

    def delete_crawled_url(self, crawled_url):
        if crawled_url is not None:
            self.session.delete(crawled_url)
        self.session.commit()
        return True

    def delete_waiting_url(self, waiting_url):
        if waiting_url is not None:
            self.session.delete(waiting_url)
        self.session.commit()
        return True


Base.metadata.create_all(engine)
