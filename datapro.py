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

'''
ub = pd.read_csv('ccf_first_round_user_shop_behavior.csv')
#test = pd.read_csv('evaluation_public.csv')

X = user_behavior[['longitude','latitude']].values
y = user_behavior['shop_id']
X_test = test[['longitude','latitude']]
X_row = list(test['row_id'])
#X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

knn = neighbors.KNeighborsClassifier(n_neighbors = 20, weights='distance')

knn.fit(X, y)

predict = list(knn.predict(X_test))

res = pd.DataFrame({'row_id': X_row,'shop_id':predict})
res.to_csv('result.csv', index = False)
#score = knn.score(X_test, y_test)
#print score
'''

f=open('C:/files/locationtheshop/malls.bin','rb')
malls=pickle.load(f)
'''
# 取出每个mall的所有wifi集合
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
'''
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
    udata=udata.join(ub[['longitude','latitude','shop_id']])
    udata.to_csv('C:/files/locationtheshop/new/'+mall+'.csv')

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

  
'''
#ub = pd.read_csv('C:/files/locationtheshop/ccf_first_round_user_shop_behavior.csv')
#ub = pd.read_csv('C:/files/locationtheshop/newub.csv')
ub = pd.read_csv('C:/files/locationtheshop/malls/m_626.csv')
test = pd.read_csv('C:/files/locationtheshop/malls/test_m_626.csv')
#test = pd.read_csv('C:/files/locationtheshop/evaluation_public.csv')
#shops=pd.read_csv('C:/files/locationtheshop/ccf_first_round_shop_info.csv')
#malls = shops['mall_id'].unique()
#f=open('C:/files/locationtheshop/malls.bin','wb')
#pickle.dump(malls,f)

ubwifi = ub.wifi_infos.apply(lambda x: list(map(lambda y: y.split('|')[0], x.split(';'))))
testwifi = test.wifi_infos.apply(lambda x: list(map(lambda y: y.split('|')[0], x.split(';'))))
wifi1=[]
for i in ubwifi:
    wifi1 = wifi1 + i
    
wifi2=[]
for i in testwifi:
    wifi2 = wifi2 + i

wifis=set(wifi1)&set(wifi2)

'''

'''
# 把每个mall的所有数据分开存储
shops=pd.read_csv('C:/files/locationtheshop/ccf_first_round_shop_info.csv')

newshop=shops[['mall_id','shop_id']]

newub=pd.merge(ub, newshop, on='shop_id', how='left', sort=False)

newub.to_csv('C:/files/locationtheshop/newub.csv')

malls=ub['mall_id'].unique()
for m in malls:
    mall626=ub[ub.mall_id == m]
    mall626_test=test[test.mall_id == m]
    mall626.to_csv('C:/files/locationtheshop/malls/'+m+'.csv')
    mall626_test.to_csv('C:/files/locationtheshop/malls/test_'+m+'.csv')
'''

