from Data import Database
import pandas as pd
import tushare as ts
import numpy as np

db,db_engine = Database.initSession()

ts.set_token('a4a8760e424d5cb0c967874df9a064595124b74af2354c0dbbe20b5d')
pro = ts.pro_api()
data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code')
stock_list = data['ts_code'].tolist()

date = '20180501'

for code in stock_list[:1000]:
    try:
        ave_sql = "select * from daily_info where code =" + "'" + code + "'" + "and date < " + "'" + date + "'"
        turnover_sql = "select * from turnover_rate where code = " + "'" + code + "'" + "and date <" + "'" + date + "'"

        ave = pd.read_sql_query(ave_sql, db_engine)
        turnover = pd.read_sql_query(turnover_sql, db_engine)


        df1 = ave[-100:]
        df2 = turnover[-100:]

        data = pd.merge(df1, df2, on='date')[['close', 'date', 'amount', 'vol', 'turnover_rate']]
        data.reset_index(drop=True, inplace=True)
        data[['close', 'amount', 'vol', 'turnover_rate']] = data[['close', 'amount', 'vol', 'turnover_rate']].astype(
            float)

        # 计算每日均价

        data.eval("""
                       ave_price = (amount * 1000) / (vol * 100)
                       turnover_weight = turnover_rate/100
                       """, inplace=True)

        np_data = np.array(data[['close', 'ave_price', 'turnover_weight']])

        length = np.shape(np_data)[0]

        for i in range(length):
            for j in range(i + 1, length):
                #data.loc[index, 'turnover_weight'] *= (1 - data.loc[j, 'turnover_weight'])
                np_data[i][2] *= (1 - np_data[j][2])

        num = np.sum(np_data[:,2])
        np_data[:,2] /= num
        RP = np.sum(np_data[:,2]*np_data[:,1])
        # data.eval("""
        #                RP =  turnover_weight * ave_price
        #                """, inplace=True)
        # RP = data['RP'].sum()
        CGO = (np_data[-1][0] - RP) / RP
        print("%s is finished" %code)
    except:
        print("%s went wrong" %code)
