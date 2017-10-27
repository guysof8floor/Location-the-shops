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
#from sklearn.cross_validation import train_test_split

# 将每个mall数据的wifi集合的特征进行处理，并添加经纬度信息。
f=open('C:/files/locationtheshop/malls.bin','rb')
malls=pickle.load(f)

for mall in malls:
    f=open('C:/files/locationtheshop/malls/wifis_'+mall+'.bin','rb')
    wifis=pickle.load(f)
    ub = pd.read_csv('C:/files/locationtheshop/malls/'+mall+'.csv')
    test = pd.read_csv('C:/files/locationtheshop/malls/test_'+mall+'.csv')
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
    udata=DataFrame(uwifi, columns=range(len(wifis)))
    udata=ub[['shop_id','longitude','latitude']].join(udata)
    udata.to_csv('C:/files/locationtheshop/new/'+mall+'.csv', index=False)

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
    tdata=DataFrame(twifi, columns=range(len(wifis)))
    tdata=tdata.join(test[['longitude','latitude','row_id']])
    tdata.to_csv('C:/files/locationtheshop/new/test_'+mall+'.csv')
