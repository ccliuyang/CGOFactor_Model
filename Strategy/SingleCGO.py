from rqalpha.api import *
from Data import Database
from rqalpha import run_func
import pandas as pd
import tushare as ts
from sklearn.preprocessing import MinMaxScaler
import time
import math


ts.set_token('a4a8760e424d5cb0c967874df9a064595124b74af2354c0dbbe20b5d')
pro = ts.pro_api()

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。

def get_QualityStock(stocks):
    df = pd.DataFrame()
    df2 = pd.DataFrame()

    for i in range(math.ceil(len(stocks)/100)):
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


def get_top5(date):
    db,db_engine = Database.initSession()
    cgo_sql = "select * from cgo_list where date =" + "'" + date + "'"
    cgo = pd.read_sql_query(cgo_sql, db_engine)
    data = cgo.sort_values(by='cgo_factor', ascending=False)
    data = data[4*int(data.shape[0]/5):5*int(data.shape[0]/5)]
    stock_list = list(data.code)
    return stock_list



def id_convert(stock_list):
    res = []
    for str in stock_list:
        #print(str[-2::])
        if str[-2::] =='SH':
            res.append(str.replace('SH','XSHG'))
            #print(str)
        else:
            res.append(str.replace('SZ','XSHE'))
            #print(res)
    return res

def init(context):
    #context.stocks = id_convert(get_top5(context.now.strftime('%Y%m%d')))
    context.stocks = ['']
    context.barcount = -1



def before_trading(context):
    context.barcount +=1
    pass


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    if context.barcount % 7 == 0:
        #time.sleep(30)
        #更新持仓股票
        #context.stocks = id_convert(get_top5(context.now.strftime('%Y%m%d')))
        stocks = get_top5(context.now.strftime('%Y%m%d'))
        print(stocks)
        context.stocks = id_convert(get_QualityStock(stocks))
        context.length = len(context.stocks)

        #清仓并购买 未考虑手续费
        if context.portfolio.positions.keys() == context.stocks:
            pass
        else:
            for stock in context.portfolio.positions.keys():
                if stock not in context.stocks:
                    order_target_value(stock,0)
            for stock in context.stocks:
                if stock not in context.portfolio.positions.keys():
                    order_target_percent(stock,1/context.length)




config = {
    "base":{
        "start_date": "20180801",
        "end_date" : "20181228",
        "benchmark":"000008.XSHG",
        "accounts":{
            "stock": 100000000
        }
    },
    "extra":{
        "log_level":"verbose",
    },
    "mod":{
        "sys_analyser":{
            "enabled":True,
            "plot":True
        }
    }
}
run_func(**globals())