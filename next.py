import os
import pandas as pd
from datetime import datetime, timedelta, timezone
import pymysql
import sqlalchemy
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdate
from playhouse.migrate import *

from peewee import *

# base = r'D:\\M\\A故障总结\\自己故障\\A32A33\\00\\立项\\完成译码\\0419\\output\\'
# base = r'D:\\M\\A故障总结\\自己故障\\A32A33\\00\\立项\\完成译码\\0408-0418\\output\\'
base = r'D:\\M\\A故障总结\\自己故障\\A32A33\\00\\立项\\完成译码\\test\\'
# base = '/Users/wm/M/A故障总结/自己故障/A32A33/00/立项/完成译码/test/'
ENG_1 = 1
ENG_2 = 2

db = MySQLDatabase(database='qar_mon',
                   host='localhost',
                   user='root',
                   password='root')


class My_ac(Model):
    class Meta:
        database = db

    ac_num = CharField(max_length=6, primary_key=True)
    eng_type = CharField()
    wireless_version = IntegerField()
    pc_version = IntegerField()


class Ac_mon(Model):
    class Meta:
        database = db
        primary_key = CompositeKey('ac_num', 'take_off_datetime')

    ac_num = ForeignKeyField(My_ac, backref='data')
    take_off_datetime = DateTimeField()
    apu_oil_temp_max = IntegerField()
    eng1_prv_bleed_press_mean = FloatField()
    eng2_prv_bleed_press_mean = FloatField()
    l1_up_time_sum = IntegerField()
    l2_up_time_sum = IntegerField()
    n1_up_time_sum = IntegerField()
    n2_up_time_sum = IntegerField()
    r1_up_time_sum = IntegerField()
    r2_up_time_sum = IntegerField()
    l1_down_time_sum = IntegerField()
    l2_down_time_sum = IntegerField()
    n1_down_time_sum = IntegerField()
    n2_down_time_sum = IntegerField()
    r1_down_time_sum = IntegerField()
    r2_down_time_sum = IntegerField()
    eng1_trans_pr = FloatField(default=None)
    eng2_trans_pr = FloatField(default=None)


db.create_tables([My_ac, Ac_mon])


# trans_pr = FloatField(default=None)
# migrator = MySQLMigrator(db)
# migrate(
#     migrator.add_column('Ac_mon', 'trans_pr', trans_pr)
# )


def get_file_names(base):
    for root, ds, fs in os.walk(base):
        for short_name in fs:
            if short_name.endswith('.csv'):
                full_name = os.path.join(root, short_name)
                yield full_name


def get_prv_bleed_press_mean(df, eng_num):
    hpv = 'HPV_ENG{}_R'.format(eng_num)
    prv = 'PRV_ENG{}_R'.format(eng_num)
    pr_col = 'PRECOOL_PRESS{}'.format(eng_num)
    pr = pd.to_numeric(df.loc[(df[hpv] == 'FULLY CLS') & (df[prv] == 'NOT FC')][pr_col]).mean()
    return round(pr, 1)


def get_apu_oil_temp_max(df):
    # APU_OIL_TEMP默认是object类，使用pd.to_numeric() 转换成适当数值类型
    df.APU_OIL_TEMP = pd.to_numeric(df.APU_OIL_TEMP)
    apu_oil_temp_max = df['APU_OIL_TEMP'].max()
    return apu_oil_temp_max


def store_data(base):
    for full_name in get_file_names(base):
        components = full_name.split('_')
        ac_num = components[0].split('\\')[-1]
        df = pd.read_csv(full_name)
        # 删除首行的NaN和单位
        df.drop(df.index[0], inplace=True)
        # df['full_datetime'] = df[['DATE', 'Time']].apply(' '.join, axis=1)
        # 把object类型的字段的空格删除
        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        apu_oil_temp_max = get_apu_oil_temp_max(df)
        # df3[['DATYEAR', 'DATMONTH', 'DATDAY']] = df3[['DATYEAR', 'DATMONTH', 'DATDAY']].astype(np.int64).copy()
        eng1_prv_bleed_press_mean = get_prv_bleed_press_mean(df, ENG_1)
        eng2_prv_bleed_press_mean = get_prv_bleed_press_mean(df, ENG_2)
        take_off_datetime = get_take_off_datetime(df)
        Ac_mon.create(ac_num=ac_num, \
                      apu_oil_temp_max=apu_oil_temp_max, \
                      take_off_datetime=take_off_datetime, \
                      eng1_prv_bleed_press_mean=eng1_prv_bleed_press_mean, \
                      eng2_prv_bleed_press_mean=eng2_prv_bleed_press_mean, \
                      **get_landing_gear_time(df))


def get_landing_gear_time(df):
    sel_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.AIR_GROUND == 'AIR')][0:1]
    sel_down_first_df = df[df.index == (df[(df.LDG_SELUP2 == 'UP')].index[-1] + 1)]

    l1_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_LH_NUP_L1 == 'LOCKED UP') & (
            df.AIR_GROUND == 'AIR')][0:1]
    l2_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_LH_NUP_L2 == 'LOCKED UP') & (
            df.AIR_GROUND == 'AIR')][0:1]
    n1_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_NS_NUP_L1 == 'LOCKED UP') & (
            df.AIR_GROUND == 'AIR')][0:1]
    n2_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_NS_NUP_L2 == 'LOCKED UP') & (
            df.AIR_GROUND == 'AIR')][0:1]
    r1_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_RH_NUP_L1 == 'LOCKED UP') & (
            df.AIR_GROUND == 'AIR')][0:1]
    r2_lock_up_first_df = df[(df.LDG_SELDW2 == 'NOT DOWN') & (df.GEAR_RH_NUP_L2 == 'LOCKED UP') & (
            df.AIR_GROUND == 'AIR')][0:1]

    l1_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_LH_DWLK1 == 'NOT LK DN') & (
                df.AIR_GROUND == 'AIR')].index[-1] + 1)]
    l2_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_LH_DWLK2 == 'NOT LK DN') & (
                df.AIR_GROUND == 'AIR')].index[-1] + 1)]
    n1_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_NS_DWLK1 == 'NOT LK DN') & (
                df.AIR_GROUND == 'AIR')].index[-1] + 1)]
    n2_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_NS_DWLK2 == 'NOT LK DN') & (
                df.AIR_GROUND == 'AIR')].index[-1] + 1)]
    r1_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_RH_DWLK1 == 'NOT LK DN') & (
                df.AIR_GROUND == 'AIR')].index[-1] + 1)]
    r2_lock_down_first_df = df[
        df.index == (df[(df.LDG_SELUP2 == 'NOT UP') & (df.GEAR_RH_DWLK2 == 'NOT LK DN') & (
                df.AIR_GROUND == 'AIR')].index[-1] + 1)]

    def get_datetime(df):
        d = (df.DATE.iloc[0] + ' ' + df.Time.iloc[0]).strip()
        dt = datetime.strptime(d, '%d/%m/%y %H:%M:%S')
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


def show_data():
    engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/qar_mon')
    df = pd.read_sql_table('test', engine)
    # print(df.groupby('ac_num').agg({'apu_oil_temp_max': max}))
    # print(df.groupby('ac_num')['apu_oil_temp_max'])
    df.set_index('take_off_datetime', inplace=True)
    df.groupby('ac_num')['eng1_prv_bleed_press_mean'].plot(legend=True)
    plt.show()


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


def trendline(data):
    order = 1
    index = [i for i in range(1, len(data) + 1)]
    coeffs = np.polyfit(index, list(data), order)
    slope = coeffs[-2]
    return float(slope)


def utc_to_zh(dt):
    dt_zh = dt + timedelta(hours=8)
    return dt_zh


def main():
    store_data(base)
    # show_data()
    # pass


if __name__ == '__main__':
    main()
