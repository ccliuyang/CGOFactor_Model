from Data.config import *
from Data import Database
import pandas as pd
import matplotlib.pyplot as plt
import tushare as ts
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import StandardScaler

def get_top5(date):
    db,db_engine = Database.initSession()
    cgo_sql = "select * from cgo_list where date =" + "'" + date + "'"
    cgo = pd.read_sql_query(cgo_sql, db_engine)
    data = cgo.sort_values(by='cgo_factor', ascending=False)
    data = data[0*int(data.shape[0]/5):1*int(data.shape[0]/5)]
    stock_list = list(data.code)
    return stock_list


ts.set_token('a4a8760e424d5cb0c967874df9a064595124b74af2354c0dbbe20b5d')
pro = ts.pro_api()
stocks = get_top5('20180101')
#assets_yoy
#df = pro.fina_indicator(ts_code='600000.SH',fields='ts_code,ann_date,assets_yoy')
def get_QualityStock(stocks):
    df = pd.DataFrame()
    df2 = pd.DataFrame()

    for i in range(int(len(stocks)/100)+1):
        if i != int(len(stocks)/100):
            df = df.append(pro.query('fina_indicator', ts_code=','.join(stocks[i*100:(i+1)*100]), start_date='20170801', end_date='20171228',fields = 'ts_code,assets_yoy,roe'))
            df2 = df2.append(pro.query('daily_basic', ts_code=','.join(stocks[i*100:(i+1)*100]), trade_date='20170825',fields='ts_code,dv_ratio').fillna(0))
        else:
            df = df.append(pro.query('fina_indicator', ts_code=','.join(stocks[i * 100:]), start_date='20170801',end_date='20171228',fields='ts_code,assets_yoy,roe'))
            df2 = df2.append(pro.query('daily_basic', ts_code=','.join(stocks[i * 100:]), trade_date='20170825',fields='ts_code,dv_ratio').fillna(0))

    data = pd.merge(df,df2)
    data[['dv_ratio','assets_yoy','roe']] = MinMaxScaler().fit_transform(data[['dv_ratio','assets_yoy','roe']])


    data.eval("""
                           score = dv_ratio+assets_yoy+roe
                           """, inplace=True)
    data = data.sort_values(by='score',ascending=False)
    return list(data.ts_code[:int(data.shape[0]/5)])
print(get_QualityStock(stocks))