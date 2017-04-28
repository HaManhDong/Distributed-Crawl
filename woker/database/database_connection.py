# import sqlalchemy
import os

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
FOLDER_PATH = os.path.dirname(os.path.abspath(__file__))


class NewsData(Base):
    __tablename__ = 'news'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    title = Column(String)
    time = Column(String)
    type = Column(String)
    content = Column(String)


class WorkerNodeIp(Base):
    __tablename__ = 'WorkerNodeIp'
    id = Column(Integer, primary_key=True)
    ip = Column(String)


class UrlSet(Base):
    __tablename__ = "UrlSet"
    id = Column(Integer, primary_key=True)
    url = Column(String)


class NewsItem:
    def __init__(self, url, title, summary, content):
        self.url = url
        self.title = title
        self.summary = summary
        self.content = content


def create_database_and_connect(database_name):
    engine = create_engine('sqlite:///' + FOLDER_PATH + '/' + database_name + '.sqlite')
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
