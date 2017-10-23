#codeing = utf-8
from sklearn import neighbors
import sklearn
import pandas as pd 
from sklearn.cross_validation import train_test_split

user_behavior = pd.read_csv('ccf_first_round_user_shop_behavior.csv')
test = pd.read_csv('evaluation_public.csv')

X = user_behavior[['longitude','latitude']].values
y = user_behavior['shop_id']
X_test = test[['longitude','latitude']]
X_row = list(test['row_id'])
# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

knn = neighbors.KNeighborsClassifier(n_neighbors = 1, weights='uniform')

knn.fit(X, y)

predict = list(knn.predict(X_test))

res = pd.DataFrame({'row_id': X_row,'shop_id':predict})
res.to_csv('result.csv', index = False)
# score = knn.score(X_test, y_test)
# print score

