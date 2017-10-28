# -*- coding: utf-8 -*-
"""
Created on Fri Oct 27 14:39:38 2017
@author: linwanying
"""

import pandas as pd 
from pandas import DataFrame
import pickle
#from sklearn.cross_validation import train_test_split

#根据自己的目录修改localpath之后使用
localpath = "/"

# 把每个mall的所有数据分开存储
shops=pd.read_csv(localpath+'ccf_first_round_shop_info.csv')
ub = pd.read_csv(localpath+'ccf_first_round_user_shop_behavior.csv')

newshop=shops[['mall_id','shop_id']]
newub=pd.merge(ub, newshop, on='shop_id', how='left', sort=False)

newub.to_csv(localpath+'newub.csv')
test = pd.read_csv(localpath+'evaluation_public.csv')

# 保存所有商场信息
malls=shops['mall_id'].unique()
f=open(localpath+'malls.bin','wb')
pickle.dump(malls,f)
f.close()

i = len(malls)
for m in malls:
    i -= 1
    print(i)
    malla=newub[newub.mall_id == m]
    malla_test=test[test.mall_id == m]
    malla.to_csv(localpath+'malls/'+m+'.csv')
    malla_test.to_csv(localpath+'malls/test_'+m+'.csv')
