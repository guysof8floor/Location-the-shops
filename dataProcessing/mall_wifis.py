# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 14:42:35 2017
@author: linwanying
"""
import pandas as pd 
import pickle

#根据自己的目录修改localpath之后使用
localpath = "/"

f=open(localpath+'malls.bin','rb')
malls=pickle.load(f)

# 取出每个mall的训练集和评估集的所有wifi集合的交集
j = len(malls)
for mall in malls:
    j = j - 1
    print(j)
    ub = pd.read_csv(localpath+'malls/'+mall+'.csv')
    test = pd.read_csv(localpath+'malls/test_'+mall+'.csv')
    ubwifi = ub.wifi_infos.apply(lambda x: list(map(lambda y: y.split('|')[0], x.split(';'))))
    testwifi = test.wifi_infos.apply(lambda x: list(map(lambda y: y.split('|')[0], x.split(';'))))
    wifi1=[]
    for i in ubwifi:
        wifi1 = wifi1 + i
    wifi2=[]
    for i in testwifi:
        wifi2 = wifi2 + i
    wifis=set(wifi1)&set(wifi2)
    f1 = open(localpath+'malls/wifis_'+mall+'.bin','wb')
    pickle.dump(wifis,f1)
