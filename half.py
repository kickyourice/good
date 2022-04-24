from distutils import errors
from tkinter import W
import pandas as pd
import numpy as np
# import MySQLdb

from sqlalchemy import create_engine
import os
from pypika import Query, Table, Field
import matplotlib.pyplot as plt

year = {
    20:2020,
    21:2021,
    22:2022
}
base = '/Users/wm/M/A故障总结/自己故障/A32A33/00/立项/完成译码/test/'
def findAllFile(base):
    for root, ds, fs in os.walk(base):
        for f in fs:
            if f.endswith('.csv'):
                fullname = os.path.join(root, f)
                yield fullname

def acTailGetter(f):
    return os.path.basename(f).split('_')[0]

files = findAllFile(base)
def get_time_period():
    acTail = input("actail:")
    start_date= input("start date:")
    # end_date = input("end date:")
    start_utc = pd.Timestamp(start_date)
    end_utc = pd.Timestamp(start_date)+pd.Timedelta('90 minutes')
    return acTail, start_utc, end_utc
def query_data(engine):
    acTail, start_utc, end_utc = get_time_period()
    sql = 'select * from bleed_data where UTC >= '+'\''+str(start_utc)+'\''+' and UTC <= ' +'\''+str(end_utc)+'\''+\
        ' and '+'AC_TAIL = '+ '\''+acTail+'\'';
    # bleed_data = Table('bleed_data')
    # sql = Query.from_(bleed_data).select('*').where(Field(UTC)[str(start_utc) : str(end_utc)])
    df = pd.read_sql_query(sql, engine)
    return df
for f in files:
    acTail = acTailGetter(f)
    df = pd.read_csv(f, skiprows= lambda x : x in[1, 2])
# 这里index里的角标代表第10个元素，原来是0，由于有的飞机在第一个ENG START出现的时候DAY不对，故向后挪动10，后续观察
    eng_start_index = df[df.FLIGHT_PHASE == 'ENG START'].index[10]
    df2 = df.loc[:eng_start_index].copy()
    df2.fillna(method='ffill', inplace=True)
    df3 = df2.dropna(subset=['DATYEAR', 'DATMONTH', 'DATDAY','UTCHR', 'UTCMIN', 'UTCSEC']).copy()
    # 取出第一个ENG START所在的一行
    df4 = df3.loc[df.FLIGHT_PHASE == 'ENG START'].copy()
    # 将float格式转化为int
    df5 = df4[['DATYEAR', 'DATMONTH', 'DATDAY','UTCHR', 'UTCMIN', 'UTCSEC']].astype(np.int64).copy()
    # 将21变成2021
    df5['DATYEAR'] = df5['DATYEAR'].map(year)
    df6 = df5.astype(str).copy()
    df6['NEW_DATE'] = df6[['DATYEAR', 'DATMONTH', 'DATDAY']].apply('-'.join, axis=1)
    df6['NEW_TIME'] = df6[['UTCHR', 'UTCMIN', 'UTCSEC']].apply(':'.join, axis=1)
    # 生成start_time
    start_time = df6.loc[eng_start_index].NEW_DATE +' '+ df6.loc[eng_start_index].NEW_TIME
    # 用先前的df来生成从第一个ENG START开始的所有数据
    df7 = df.loc[eng_start_index:, :].copy()
    # 新增一列，内容为第一个ENG START对应的自己生成的时间，用作id
    t = pd.date_range(start_time, periods=len(df7), freq='S', tz='UTC').copy()
    t2 = t.strftime("%Y-%m-%d %H:%M:%S")
    # 新增一列，内容为自己生成的UTC时间，一秒一次
    df7['UTC'] = t2
    df7['AC_TAIL'] = acTail
    # df7.to_excel('/Users/wm/M/A故障总结/自己故障/A32A33/00/立项/完成译码/test/output.xlsx', index = False)
    # df7.to_excel('/Users/wm/M/A故障总结/自己故障/A32A33/00/立项/完成译码/test/output.xlsx', index = False)
    # df7[['UTC', 'PRECOOL_PRESS1', 'PRECOOL_PRESS1']].to_csv('/Users/wm/M/A故障总结/自己故障/A32A33/00/立项/完成译码/test/output.csv')
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', 'root', 'localhost', '3306', 'qar_data'))
    real_columns = ['Time', 'FLIGHT_PHASE', 'DATYEAR', 'DATMONTH', 'DATDAY',
       'UTCHR', 'UTCMIN', 'UTCSEC', 'N21_DMC', 'N22_DMC', 'PRECOOL_PRESS1',
       'PRECOOL_PRESS2', 'PRECOOL_TEMP1', 'PRECOOL_TEMP2', 'OUT_FLOW_V1',
       'OUT_FLOW_V2', 'UTC', 'AC_TAIL']
    df7[real_columns].to_sql('bleed_data', engine, index=False, if_exists='append')
o = query_data(engine)
o[['PRECOOL_PRESS1', 'PRECOOL_PRESS2']].plot()
plt.show()