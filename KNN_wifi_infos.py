#coding:utf-8
import random
import math
import pandas as pd 
from sklearn.cross_validation import train_test_split

# 提取训练集与测试集数据
def extracted_data():
    user_behavior = pd.read_csv('ccf_first_round_user_shop_behavior.csv')
    test = pd.read_csv('evaluation_public.csv')
    X = user_behavior['wifi_infos']
    y = user_behavior['shop_id']
    X_test = test['wifi_infos']
    X_row = list(test['row_id'])
    print'len train:',len(X),'len test:',len(X_test)
    return X,y,X_test,X_row


def euclideanDistance(instance1, instance2):
    distance = 0
    #对应AP作差  
    set1 = set()
    set2 = set()
    for wifi in instance1.split(';') :
        set1.add(wifi.split('|')[0])
    for wifi in instance2.split(';') :
        set2.add(wifi.split('|')[0])
    distance = len(set1-set2)+len(set2-set1)
    return math.sqrt(distance)

#求出最近的几个邻居点
def Neighbors(trainingSet, testInstance, k):
    distances = []
    label_ID=[]
   #生成训练数据集个数的序号label_ID=0,1,2,3,4,5,6,7.。。。。。。。
    for ii in range(len(trainingSet)):
        label_ID.append(ii)
    #遍历testInstance与trainingSet每个案例的距离，所得距离值存入distances
    for x in range(len(trainingSet)):
        dist = euclideanDistance(testInstance, trainingSet[x])
        distances.append(dist)
    #对distances进行冒泡升值排序，同时把对应的序号label_ID进行相应排序（训练集的顺序 是 固定的）
    for ii in range(len(trainingSet)-1):
      for jj in range(len(trainingSet)-ii-1):
        if distances[jj]>distances[jj+1]:
           distances[jj],distances[jj+1]=distances[jj+1],distances[jj]
           label_ID[jj],label_ID[jj+1]=label_ID[jj+1],label_ID[jj]
    #将最近的k个邻居样本存下来，上面的代码已经进行了排序因此最近的 就是 最前面的k个
    neighbors = []
    for x in range(k):
        neighbors.append(label_ID[x])
    return neighbors

def Result(neighbors,label):
    length = len(neighbors)
    result = []
    for i in range(length):
        result.append(label[neighbors[i]])
    result = pd.value_counts(result, sort=True)
    return result.index[0]

def Accuracy(predictions,test_label):
    accuracy=0.0
    for i in range(len(predictions)):
        accuracy += int(predictions[i] == test_label[i])
    return accuracy/len(predictions)


if __name__=='__main__':

    trainingSet,train_label,testSet,test_label=extracted_data()
    predictions=[]
    k = 3
    for x in range(len(testSet)):
        # if(x%100 == 0):
        print x 
        neighbors = Neighbors(trainingSet, testSet[x], k)
        result = Result(neighbors,train_label)
        predictions.append(result)
    predict = list(predictions)
    res = pd.DataFrame({'row_id': X_row,'shop_id':predict})
    res.to_csv('result3.csv', index = False)