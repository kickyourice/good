from peewee import *
import pandas as pd
from datetime import datetime, timedelta, timezone
from datetime import date
import numpy as np

df = pd.read_csv('9948_20220411_1.csv')
df['full_datetime'] = df['DATE'].map(str) + ' ' + df['Time'].map(str)
# df.dropna()['full_datetime'].apply(lambda x:datetime.strptime(x, '%d/%m/%y %H:%M:%S'))
# print(df.head())
# d = datetime.strptime('10/04/22 23:25:18', '%d/%m/%y %H:%M:%S')
# df['full_datetime_x'] = df['full_datetime'].dropna().apply(lambda x:datetime.strptime(x, '%d/%m/%y %H:%M:%S'))

def get_landing_gear_time(df):
    sel_up_first_df = df[df.LDG_SELDW2 == 'NOT DOWN'][0:1]
    sel_down_first_df = df[df.index == (df[(df.LDG_SELUP2 == 'UP')].index[-1] + 1)]

    l1_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_LH_NUP_L1 == 'LOCKED UP')][0:1]
    l2_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_LH_NUP_L2 == 'LOCKED UP')][0:1]
    n1_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_NS_NUP_L1 == 'LOCKED UP')][0:1]
    n2_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_NS_NUP_L2 == 'LOCKED UP')][0:1]
    r1_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_RH_NUP_L1 == 'LOCKED UP')][0:1]
    r2_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_RH_NUP_L2 == 'LOCKED UP')][0:1]

    l1_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_LH_DWLK1 == 'NOT LK DN')].index[-1] + 1)]
    l2_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_LH_DWLK2 == 'NOT LK DN')].index[-1] + 1)]
    n1_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_NS_DWLK1 == 'NOT LK DN')].index[-1] + 1)]
    n2_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_NS_DWLK2 == 'NOT LK DN')].index[-1] + 1)]
    r1_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_RH_DWLK1 == 'NOT LK DN')].index[-1] + 1)]
    r2_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_RH_DWLK2 == 'NOT LK DN')].index[-1] + 1)]

    def get_datetime(df):
        d = (df.DATE.iloc[0] + ' ' + df.Time.iloc[0]).strip()
        dt = datetime.strptime(d, '%m/%d/%y %H:%M:%S')
        return dt

    sel_up_first_datetime = get_datetime(sel_up_first_df)
    sel_down_first_datetime = get_datetime(sel_down_first_df)

    l1_up_time_sum = (get_datetime(l1_lock_up_first_df) - get_datetime(sel_up_first_df)).seconds
    l2_up_time_sum = (get_datetime(l2_lock_up_first_df) - get_datetime(sel_up_first_df)).seconds
    n1_up_time_sum = (get_datetime(n1_lock_up_first_df) - get_datetime(sel_up_first_df)).seconds
    n2_up_time_sum = (get_datetime(n2_lock_up_first_df) - get_datetime(sel_up_first_df)).seconds
    r1_up_time_sum = (get_datetime(r1_lock_up_first_df) - get_datetime(sel_up_first_df)).seconds
    r2_up_time_sum = (get_datetime(r2_lock_up_first_df) - get_datetime(sel_up_first_df)).seconds

    l1_down_time_sum = (get_datetime(l1_lock_down_first_df) - get_datetime(sel_down_first_df)).seconds
    l2_down_time_sum = (get_datetime(l2_lock_down_first_df) - get_datetime(sel_down_first_df)).seconds
    n1_down_time_sum = (get_datetime(n1_lock_down_first_df) - get_datetime(sel_down_first_df)).seconds
    n2_down_time_sum = (get_datetime(n2_lock_down_first_df) - get_datetime(sel_down_first_df)).seconds
    r1_down_time_sum = (get_datetime(r1_lock_down_first_df) - get_datetime(sel_down_first_df)).seconds
    r2_down_time_sum = (get_datetime(r2_lock_down_first_df) - get_datetime(sel_down_first_df)).seconds

    cycle_time_dict = {
        'l1_up_time_sum': l1_up_time_sum,
        'l2_up_time_sum': l2_up_time_sum,
        'n1_up_time_sum': n1_up_time_sum,
        'n2_up_time_sum': n2_up_time_sum,
        'r1_up_time_sum': r1_up_time_sum,
        'r2_up_time_sum': r2_up_time_sum,
        'l1_down_time_sum': l1_down_time_sum,
        'l2_down_time_sum': l2_down_time_sum,
        'n1_down_time_sum': n1_down_time_sum,
        'n2_down_time_sum': n2_down_time_sum,
        'r1_down_time_sum': r1_down_time_sum,
        'r2_down_time_sum': r2_down_time_sum,
    }
    return cycle_time_dict


db = MySQLDatabase(database='test',
                   host='localhost',
                   user='root',
                   password='root')

db.connect()


class BaseModel(Model):
    class Meta:
        database = db


class User(BaseModel):
    username = CharField(unique=True)


class Tweet(BaseModel):
    user = ForeignKeyField(User, backref='tweets')
    msg = TextField()
    create_date = DateTimeField(default=datetime.now())
    is_published = BooleanField(default=True)


class My_ac(BaseModel):
    ac_num = CharField(max_length=45)
    take_off_datetime = DateTimeField()
    apu_oil_temp_max = IntegerField()
    eng1_prv_bleed_press_mean = FloatField()
    eng2_prv_bleed_press_mean = FloatField()
    l1_up_time_sum = IntegerField()
    l2_up_time_sum = IntegerField()
    n1_up_time_sum = IntegerField()
    n2_up_time_sum = IntegerField()
    r1_up_time_sum = IntegerField()
    r1_up_time_sum = IntegerField()
    l1_down_time_sum = IntegerField()
    l2_down_time_sum = IntegerField()
    n1_down_time_sum = IntegerField()
    n2_down_time_sum = IntegerField()
    r1_down_time_sum = IntegerField()
    r2_down_time_sum = IntegerField()


def get_take_off_datetime(df):
    df2 = df[df.FLIGHT_PHASE == 'TAKE OFF'][0:1]
    df2['fd'] = df2['DATE'] + ' ' + df2['Time']
    # df['fd'] = df['full_datetime'].dropna(how = 'all')
    df2['full_datetime'] = pd.to_datetime(df2['fd'], dayfirst=True)
    take_off_datetime = df2['full_datetime'].reset_index(drop=True)[0]
    # df.fillna(method='ffill')
    # df2 = df.loc[:, ['FLIGHT_PHASE', 'Time', 'DATYEAR', 'DATMONTH', 'DATDAY']]
    # df2.fillna(method='ffill', inplace=True)
    # df3 = df2.dropna().copy()
    # df3[['DATYEAR', 'DATMONTH', 'DATDAY']] = df3[['DATYEAR', 'DATMONTH', 'DATDAY']].astype(np.int64).copy()
    # year = {
    #     20: 2020,
    #     21: 2021,
    #     22: 2022
    # }
    # df3['DATYEAR'] = df3['DATYEAR'].map(year)
    # df4 = df3.astype(str).copy()
    # df4['NEW_DATE'] = df4.loc[:, 'DATYEAR':'DATDAY'].apply('-'.join, axis=1)
    # take_off_dt_utc = df4[df4.FLIGHT_PHASE == 'TAKE OFF'][0:1].NEW_DATE + \
    #                   ' ' + df4[df4.FLIGHT_PHASE == 'TAKE OFF'][0:1].Time
    # take_off_datetime_utc = datetime.strptime(take_off_dt_utc.reset_index(drop=True)[0], '%Y-%m-%d %H:%M:%S')
    return take_off_datetime

if __name__ == '__main__':
    # db.create_tables([My_ac])
    # User.create(username='zhangsan')
    # Tweet.create(user_id = 'zhangsan', msg = 'hello world')
    # d = get_landing_gear_time(df)
    # My_ac.create(**d)
    pass