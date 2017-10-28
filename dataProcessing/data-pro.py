# -*- coding: utf-8 -*-
"""
Spyder Editor
This is a temporary script file.
"""

#codeing = utf-8
#from sklearn import neighbors
#import sklearn
import pandas as pd 
from pandas import DataFrame
import pickle
# from tqdm import tqdm
#from sklearn.cross_validation import train_test_split

#根据自己目录修改localpath之后使用
localpath = "/"

# 将每个mall数据的wifi集合的特征进行处理，并添加经纬度信息。
f=open(localpath+'malls.bin','rb')
malls=pickle.load(f)

j = len(malls)
for mall in malls:
    print(j)
    j-=1
    f=open(localpath+'malls/wifis_'+mall+'.bin','rb')
    wifis=pickle.load(f)
    ub = pd.read_csv(localpath+'malls/'+mall+'.csv')
    test = pd.read_csv(localpath+'malls/test_'+mall+'.csv')
    ubwifi = ub.wifi_infos.apply(lambda x: dict(map(lambda y: (y.split('|')[0],y.split('|')[1]), x.split(';'))))
    testwifi = test.wifi_infos.apply(lambda x: dict(map(lambda y: (y.split('|')[0],y.split('|')[1]), x.split(';'))))

    uwifi=[]
    for line in ubwifi:
        a = []
        for wifi in wifis:
            if wifi in line:
                a.append(line[wifi])
            else:
                a.append('-100')
        uwifi.append(a)
    print(len(uwifi))
    column_names = []
    for i in range(len(wifis)):
        column_names.append("wifi_"+str(i))
    udata=DataFrame(uwifi, columns=column_names)
    ubtime =DataFrame(ub.time_stamp.apply(lambda x: x.split(" ")[0].split("-")[2]).astype(int),columns=['time_stamp'])
    udata=ubtime.join(ub[['shop_id','longitude','latitude']]).join(udata)
    udata.to_csv(localpath+'new/'+mall+'.csv', index=False)

    twifi=[]
    for line in testwifi:
        a = []
        for wifi in wifis:
            if wifi in line:
                a.append(line[wifi])
            else:
                a.append('-100')
        twifi.append(a)
    print(len(twifi))
    tdata=DataFrame(twifi, columns=column_names)
    tdata=test[['row_id','longitude','latitude']].join(tdata)
    tdata.to_csv(localpath+'new/test_'+mall+'.csv',index=False)
