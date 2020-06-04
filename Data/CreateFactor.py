"""
采用Fama-French的思想进行分档然后按照市值加权进行获得因子收益率
"""
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

def get_factor():
    data = pd.read_excel('WholeData.xlsx', dtype=object)
    data[['date']] = data[['date']].astype(str)
    date_list = list(set(data.date))
    output = pd.DataFrame(columns=['date', 'factor'])
    #print(data)

    date_list.sort()

    for date in date_list[:-1]:
        df = data[data['date'] == date]
        df[['return', 'market_value']] = df[['return', 'market_value']].astype(float)
        df = df.sort_values(by='cgo_factor', ascending=True)

        top_df = df[:int(df.shape[0] / 5)]
        bot_df = df[-int(df.shape[0] / 5):]

        # top_df.eval('w_return = return * market_value',inplace=True)
        top_df['w_return'] = top_df[['return', 'market_value']].apply(lambda x: x['return'] * x['market_value'], axis=1)
        top_return = top_df['w_return'].sum() / top_df['market_value'].sum()
        # top_return = top_df['w_return'].cumsum()/top_df['market_value'].cumsum()
        #
        bot_df['w_return'] = bot_df[['return', 'market_value']].apply(lambda x: x['return'] * x['market_value'], axis=1)
        bot_return = bot_df['w_return'].sum() / bot_df['market_value'].sum()

        s = pd.Series({'date': date, 'factor': top_return - bot_return})
        print(s)
        output = output.append(s, ignore_index=True)
        #print(date + 'is finished!')

    #output.to_excel('factor_list.xlsx')
    print(output)
    #data = pd.read_excel('WholeData.xlsx', dtype=object)
    df = pd.merge(data,output, on=['date'],how='left')
    df.to_excel('RegressionData.xlsx')


get_factor()









