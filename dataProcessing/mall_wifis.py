# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 14:42:35 2017

@author: linwanying
"""
import pandas as pd 
import pickle

f=open('C:/files/locationtheshop/malls.bin','rb')
malls=pickle.load(f)

# 取出每个mall的训练集和评估集的所有wifi集合的交集
for mall in malls:
    ub = pd.read_csv('C:/files/locationtheshop/malls/'+mall+'.csv')
    test = pd.read_csv('C:/files/locationtheshop/malls/test_'+mall+'.csv')
    ubwifi = ub.wifi_infos.apply(lambda x: list(map(lambda y: y.split('|')[0], x.split(';'))))
    testwifi = test.wifi_infos.apply(lambda x: list(map(lambda y: y.split('|')[0], x.split(';'))))
    wifi1=[]
    for i in ubwifi:
        wifi1 = wifi1 + i
    wifi2=[]
    for i in testwifi:
        wifi2 = wifi2 + i
    wifis=set(wifi1)&set(wifi2)
    f1 = open('C:/files/locationtheshop/malls/wifis_'+mall+'.bin','wb')
    pickle.dump(wifis,f1)
