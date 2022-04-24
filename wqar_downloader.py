import os
import ftplib
from datetime import datetime, timedelta
import time
from interval import Interval
import logging
import os
import psutil
from tkinter import filedialog
from tkinter import *
logging.basicConfig(filename='demo.txt', level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')

qar_ftp = '10.78.4.66'
ac_list = ['B-1030','B-1031','B-1052','B-1618','B-1619','B-1620','B-1658','B-1659','B-1692','B-1849','B-1850','B-1851','B-300S','B-300T','B-302A','B-302C','B-302X','B-320W','B-320X','B-6789','B-6837','B-6865','B-6903','B-8062','B-8066','B-8069','B-8075','B-8381','B-8382','B-8389','B-8505','B-8597','B-8952','B-8953','B-9948','B-9963','B-9983','B-9987','B-9989']
ignore_ac_list = ['B-1030','B-1031','B-1692','B-302A','B-302C','B-302X','B-320W','B-320X','B-8381','B-8382','B-8597','B-8952','B-8953']

class MyFtp:
    def __init__(self, host, port=21):
        self.ftp = ftplib.FTP()
        self.ftp.connect(host, port)
        self.ftp.encoding = 'gbk'
        self.start_time = None
        self.start_time = None

    def Login(self, user, passwd):
        self.ftp.login(user, passwd)
        # print(self.ftp.welcome)

    def DownLoadFile(self, LocalFile, RemoteFile):  # 下载单个文件
        file_handler = open(LocalFile, 'wb')
        self.ftp.retrbinary('RETR ' + RemoteFile, file_handler.write)
        file_handler.close()
        return True
    def checkFileDir(self, file_name):
        rec = ""
        try:
            rec = self.ftp.cwd(file_name)   # 需要判断的元素
            self.ftp.cwd("..")   # 如果能通过路劲打开必为文件夹，在此返回上一级
        except ftplib.error_perm as fe:
            rec = fe # 不能通过路劲打开必为文件，抓取其错误信息
        finally:
            if "directory not found" in str(rec):
                return "File"
            elif "successful" in str(rec):
                return "Dir"
            else:
                return "Unknow"
    def isReqdAc(self, file_name, ac_list, ignore_ac_list):
        return (file_name in ac_list) and (file_name not in ignore_ac_list)
    def DownLoadFileTree(self, LocalDir, RemoteDir, inDir):  # 下载整个目录下的文件
        if (self.start_time is not None) and (self.end_time is not None):
            if not os.path.exists(LocalDir):
                os.makedirs(LocalDir)
            self.ftp.cwd(RemoteDir)
            # RemoteNames = self.ftp.nlst()
            RemoteNames = self.ftp.mlsd('.')
            # file = ('20210924144640.wgl', {'type': 'dir', 'modify': '20210926054057'})
            # if(inDir):
            #     print('Downloading {}'.format(LocalDir))
            for file in RemoteNames:
                Local = os.path.join(LocalDir, file[0])
                f_time_to_local = datetime.strptime(file[1]['modify'], '%Y%m%d%H%M%S') + timedelta(hours=8)
                if not inDir and not self.isReqdAc(str(file[0])[:6], ac_list, ignore_ac_list) :
                    # logging.debug(str(file[0])[:6]+' Not in required ac list'+'-'+file[0])
                    continue
                if not inDir and not f_time_to_local in Interval(self.start_time, self.end_time):
                    # logging.debug('Not in required interval'+'-'+file[0]+'-'+file[1]['modify'])
                    continue
                if ('Dir' == self.checkFileDir(file[0])):
                    print('Downloading {}-{}'.format(file[0], f_time_to_local))
                    self.DownLoadFileTree(Local, file[0], inDir=True)
                else:
                    self.DownLoadFile(Local, file[0])
            self.ftp.cwd("..")
            return

    def close(self):
        self.ftp.quit()
        input("Download complete.")

    def getInterval(self):
        while (True):
            try:
                date_1 = datetime.strptime(input('Enter start date,(eg.20210101)>'), '%Y%m%d')
                date_2 = datetime.strptime(input('Enter end date,(eg.20210101)>'), '%Y%m%d')
            except ValueError:
                print("日期格式不对")
                continue
            except KeyboardInterrupt:
                print("程序退出")
                break
            try:
                if (date_1 > date_2):
                    raise ValueError
            except ValueError:
                print('开始日期必须小于或等于结束日期，请重新输入')
            else:
                self.start_time = datetime.strptime(str(date_1.date()) + ' 05:00', '%Y-%m-%d %H:%M')
                self.end_time = datetime.strptime(str(date_2.date() + timedelta(days=1)) + ' 05:00','%Y-%m-%d %H:%M')
                break
    def get_local_path(self):
        top = Toplevel()
        local_path = str(filedialog.askdirectory(initialdir=os.getcwd(), title="Please select a folder:")).replace('/', '\\')
        top.destroy()
        return local_path
if __name__ == "__main__":
    mf = MyFtp(qar_ftp)
    mf.Login('ftp', 'ftp')
    local_path = r'C:\\test\\'
    romte_path = '.'
    mf.getInterval()
    mf.DownLoadFileTree(local_path, romte_path, inDir=False)  # 从目标目录下载到本地目录d盘
    mf.close()