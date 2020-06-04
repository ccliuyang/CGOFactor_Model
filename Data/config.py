import datetime
class db_config(object):
    sqlalchemy_address = 'mysql+mysqlconnector://root:YES@localhost:3306/factorhub'
    mysql_max_connection = 128
    mysql_insert_number = 20000
    max_process_num = 8

    def __init__(self):
        pass

internal = 100
last_start_date = '20180101'
start_date = '20190101'
temp = datetime.datetime.strptime('20170103',"%Y%m%d")
stop_time = (temp +datetime.timedelta(days=100)).strftime("%Y%m%d")


##2017-01-03-2018-12-28##