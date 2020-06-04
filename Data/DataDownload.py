"""
使用tushare获得close和turnover存入数据库
"""

import matplotlib.pyplot as plt
from Data import Database
from Data.config import *
import threading
import datetime
import pandas as pd
import math
import numpy as np

path = 'F:/毕业论文/金融/股票数据/return.xlsx'

plt.rcParams['font.sans-serif']=['SimHei']




class Factors(object):
    db,db_engine = Database.initSession()


    def __init__(self):
        self.data = None

    def getToday(self):
        self.data = pd.read_excel(path,dtype=object)
        self.data[['return']] = self.data[['return']].astype(float)

    def insertToday(self):
        engine = self.db_engine
        max_iteration = math.ceil(self.data.shape[0]/50000)
        for i in range(max_iteration):
            if i != max_iteration-1:
                self.data[i*50000:(i+1)*50000].to_sql('basic_list',con=engine,if_exists='append',index=False)
            else:
                self.data[i * 50000:].to_sql('basic_list', con=engine, if_exists='append', index=False)






class _getFactor(threading.Thread):
    def __init__(self,date):
        threading.Thread.__init__(self)
        self.date = date


    def run(self):
        print('start to get factor....',end='...')
        f = Factors()
        # f.getTodayClose()
        # f.insertClose()
        f.getToday()
        f.insertToday()
        print('Thread is finished')


"""
判断B是不是A的子集
"""
def allin(A,B):
    d =  [False for c in B if c not in A]
    if d:
        return False
    else:
        return True




if __name__ =='__main__':


    # total_threads = []
    # running_threads = []
    # max_threads = 64
    # now_time = datetime.datetime.strptime(start_date,"%Y%m%d")
    # for i in range(365):
    #     trade_date = (now_time-datetime.timedelta(days=i)).strftime('%Y%m%d')
    #     if trade_date == last_start_date:
    #         break
    #     else:
    #         total_threads.append(_getFactor(trade_date))
    # st = 0
    # while st < len(total_threads):
    #     if len(running_threads) < max_threads:
    #         total_threads[st].start()
    #         running_threads.append(total_threads[st])
    #         st += 1
    #         continue
    #     for t in running_threads:
    #         if not t.is_alive():
    #             running_threads.remove(t)
    # for t in total_threads:
    #     t.join()
    # f = Factors()
    # f.getToday()
    #print(f.data)

    #获取时间交集，保证全部时间的交集一致

    db,db_engine = Database.initSession()

    basic = pd.read_excel(path,dtype=object)

    code_list = "select distinct code from basic_list"
    code_list = list(pd.read_sql_query(code_list,db_engine).code)

    error_code=[]

    for code in code_list:
        #date_sql = "select distinct date from basic_list where code =" + "'"+code+"'"
        #date_list = list(pd.read_sql_query(date_sql, db_engine).date)
        date_list = list(basic[basic['code'] == code].date)
        if len(date_list) > 1900:
            pass
            #print(allin(date_list,date_intersection))
        else:
            error_code.append(code)








    """暂时不用"""
    date_list2=[]
    for code in code_list:
        date_sql = "select distinct date from cgo_list where code =" + "'"+code+"'"
        date = list(pd.read_sql_query(date_sql, db_engine).date)
        #print(date)
        if len(date_list2)==0:
            date_list2 = date
        elif len(date)>1300:
            date_list2 = list(set(date_list2).intersection(set(date)))
        else:
            error_code.append(code)

    # date_list3 = list(set(date_list).intersection(set(date_list2)))
    #
    # date_list3.sort()
    # print(date_list3,len(date_list3))
    # print(len(error_code))


    cgo_sql = "select * from cgo_list"
    cgo = pd.read_sql_query(cgo_sql,db_engine)

    #
    #
    #
    # """等待检验"""
    #
    #
    #
    #
    codes = list(set(code_list) ^set(error_code))

    cgo = cgo[cgo['code'].isin(codes)]
    cgo = cgo[cgo['date'].isin(date_list2)]

    print(len(codes)*len(date_list2))



    #
    # df = pd.read_excel('F:/毕业论文/金融/股票数据/return.xlsx',dtype=object)
    #
    # df = df[df['code'].isin(list(codes))]
    #
    # df = df[df['date'].isin(date_intersection)]
    #
    # print(df)
    #
    #
    #
    #
    # #CGO的数量没有问题
    # cgo = cgo[cgo['code'].isin(codes)]
    # print(cgo)
    #
    # cgo = cgo[cgo['date'].isin(date_intersection)]
    # print(cgo)



    df = pd.merge(cgo,basic,on=['code','date'],how='left')
    df.fillna(0,inplace=True)


    #df1= pd.read_excel('Data1.xlsx',dtype=object)

    df2 = pd.read_excel('F:/毕业论文/金融/股票数据/MarketValue.xlsx',dtype=object)
    df = pd.merge(df,df2,on=['code','date'],how='left')
    df.fillna(axis=0,method='ffill',inplace=True)
    df.to_excel('WholeData.xlsx')

    # if():
    #     df.fillna(axis=0, method='bfill', inplace=True)
    #
    # if(df.isnull().any()):
    #
    #     print('success!')
    # else:
    #     print('failed!')











