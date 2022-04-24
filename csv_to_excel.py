import os
import numpy as np
import pandas as pd

dtype={
        'PRECOOL_PRESS1':np.float64,
        'PRECOOL_PRESS2':np.float64,
        'PRECOOL_TEMP1':np.float64,
        'PRECOOL_TEMP2':np.float64
    }
def findAllFile(base):
    for root, ds, fs in os.walk(base):
        for f in fs:
            if f.endswith('.csv'):
                fullname = os.path.join(root, f)
                yield fullname
def main():
    # base = r'D:\M\A故障总结\自己故障\A32A33\00\立项\完成译码\test'
    base = r'D:\\M\\A故障总结\\自己故障\\A32A33\\00\\立项\\完成译码\\3月\\0328\\output\\'
    # base = '/Users/wm/M/A故障总结/自己故障/A32A33/00/立项/完成译码/0120/'
    
    for f in findAllFile(base):
        # new_name = os.path.join(os.path.split(f)[0], (os.path.split(f)[1]).split('.')[0]+'.xlsx')
        new_name = f.split('.')[0]+'.xlsx'
        # 如果设置skip参数，所有数据都是object,设置了skip参数后，默认都识别正确，可以单独设置一个列为str看效果
        # df = pd.read_csv(f, skiprows=[1, 2], dtype={'PRECOOL_PRESS1':str})
        df = pd.read_csv(f, skiprows=[1, 2])
        #可以不加use_cols参数，默认读取所有列
        # na_filte设定False后不检查空值，可提升读取速度
        # df = pd.read_csv(f, skiprows=[1, 2], usecols=['PRECOOL_PRESS2', 'PRECOOL_PRESS1', 'PRECOOL_TEMP2', 'PRECOOL_TEMP1'], na_filter=False)
        df.to_excel(new_name, index=False)
        # print(df.info())
if __name__ == '__main__':
    main()