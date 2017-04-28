import os
from sqlalchemy import Column, String, Integer
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

Base = declarative_base()
FOLDER_PATH = os.path.dirname(os.path.abspath(__file__))


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


engine = create_engine('sqlite:///' + FOLDER_PATH + '/data.sqlite')

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
Base = declarative_base()
Base.query = db_session.query_property()
