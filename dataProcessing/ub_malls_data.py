# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 14:39:38 2017

@author: linwanying
"""

import pandas as pd 
from pandas import DataFrame
import pickle
#from sklearn.cross_validation import train_test_split

# 把每个mall的所有数据分开存储
shops=pd.read_csv('C:/files/locationtheshop/ccf_first_round_shop_info.csv')
ub = pd.read_csv('C:/files/locationtheshop/ccf_first_round_user_shop_behavior.csv')

newshop=shops[['mall_id','shop_id']]
newub=pd.merge(ub, newshop, on='shop_id', how='left', sort=False)

newub.to_csv('C:/files/locationtheshop/newub.csv')
test = pd.read_csv('C:/files/locationtheshop/evaluation_public.csv')

# 保存所有商场信息
malls=shops['mall_id'].unique()
f=open('C:/files/locationtheshop/malls.bin','wb')
pickle.dump(malls,f)
f.close()

for m in malls:
    malla=newub[newub.mall_id == m]
    malla_test=test[test.mall_id == m]
    malla.to_csv('C:/files/locationtheshop/malls/'+m+'.csv')
    malla_test.to_csv('C:/files/locationtheshop/malls/test_'+m+'.csv')
