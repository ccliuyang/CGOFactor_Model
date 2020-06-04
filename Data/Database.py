from sqlalchemy import Column, String, create_engine, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import ProgrammingError, DatabaseError
from Data.config import db_config

# #创建对象的基类
Base = declarative_base()


def checkDatabase(conn=db_config.sqlalchemy_address):
    try:
        engine = create_engine(conn)
        DBSession = sessionmaker(bind=engine)
        dbs = DBSession()
        dbs.query(cgo_list).first()
        return (True, 0, "")
    except ProgrammingError as err:
        return (False, err.code, err.orig)
    except DatabaseError as err:
        return (False, err.code, err.orig)

def initSession(conn = db_config.sqlalchemy_address):

    engine = create_engine(conn, pool_size=db_config.mysql_max_connection)
    DBSession = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    return DBSession, engine

def createTables(conn = db_config.sqlalchemy_address):
    engine = create_engine(conn)
    Base.metadata.create_all(engine)




class cgo_list(Base):
    #Table name:
    __tablename__ = 'cgo_list'
    #Table structure:
    code = Column(String(64),primary_key=True)
    date = Column(String(64),primary_key=True)
    cgo_factor  = Column(Float, index=True)
#
# class daily_info(Base):
#     #Table name:
#     __tablename__ ='daily_info'
#     code = Column(String(64),primary_key=True)
#     date = Column(String(64),primary_key=True)
#     close = Column(String(64), index=True)
#     amount = Column(String(64), index=True)
#     vol = Column(String(64), index=True)
#
#
# class turnover_rate(Base):
#     #Table name:
#     __tablename__ ='turnover_rate'
#     code = Column(String(64),primary_key=True)
#     date = Column(String(64),primary_key=True)
#     turnover_rate = Column(String(64), index=True)
