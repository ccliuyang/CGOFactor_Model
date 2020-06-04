"""
计算CGO因子值存入数据库
"""
from Data import Database
from sqlalchemy.orm import scoped_session
import pandas as pd
import tushare as ts
import numpy as np
from Data.config import *
import threading



date_path='F:/毕业论文/金融/date.csv'

# # 计算每日CGO数据的类
class DailyStocks(object):
    db,db_engine = Database.initSession()

    def __init__(self,date):
        self.date = date
        self.CGO = {}
        self.stock_list = list(pd.read_sql_query("select distinct code from basic_list",self.db_engine).code)

    def get_cgo(self):

        for code in self.stock_list:
            try:
                # 找到当前日期，获得当前日期以前的100个数据

                sql = "select * from basic_list where code =" + "'" + code + "'" + "and date < " + "'" + self.date + "'"

                data = pd.read_sql_query(sql, self.db_engine)

                data = data[-100:]


                data.reset_index(drop=True, inplace=True)

                # 计算每日均价


                np_data = np.array(data[['close_price', 'ave_price', 'turnover_rate']])

                length = np.shape(np_data)[0]

                for i in range(length):
                    for j in range(i + 1, length):
                        # data.loc[index, 'turnover_weight'] *= (1 - data.loc[j, 'turnover_weight'])
                        np_data[i][2] *= (1 - np_data[j][2])

                num = np.sum(np_data[:, 2])
                np_data[:, 2] /= num
                RP = np.sum(np_data[:, 2] * np_data[:, 1])
                # data.eval("""
                #                RP =  turnover_weight * ave_price
                #                """, inplace=True)
                # RP = data['RP'].sum()
                self.CGO[code] = (np_data[-1][0] - RP) / RP
            except:
                pass

    def insertCGO(self):
        ss = scoped_session(self.db)
        dbs = ss()
        data = self.CGO.items()
        dbs.execute(Database.cgo_list.__table__.insert(),
                    [{"date": self.date, "code": element[0], "cgo_factor": float(element[1])} for
                     element in data])
        dbs.commit()
        dbs.close()
        ss.remove()

# #多线程函数
class _getFactor(threading.Thread):
    def __init__(self,date):
        threading.Thread.__init__(self)
        self.date = date

    def run(self):
        f = DailyStocks(self.date)
        f.get_cgo()
        f.insertCGO()
        print('Thread is finished:'+self.date)


def MutilThreadDownload(date_list):
    total_threads = []
    running_threads = []
    max_threads = 32

    for date in date_list[:]:
        total_threads.append(_getFactor(date))

    st = 0
    while st < len(total_threads):
        if len(running_threads) < max_threads:
            total_threads[st].start()
            running_threads.append(total_threads[st])
            st += 1
            continue
        for t in running_threads:
            if not t.is_alive():
                running_threads.remove(t)
    # for t in total_threads:
    #     t.join()


if __name__=='__main__':
    date_list = list(pd.read_csv(date_path).date)[100:]
    date_list = [datetime.datetime.strptime(x,'%Y/%m/%d') for x in date_list]
    date_list = [datetime.datetime.strftime(x,'%Y-%m-%d') for x in date_list]
    print(date_list)
    MutilThreadDownload(date_list[1:])






