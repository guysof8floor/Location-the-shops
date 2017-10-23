# userLocation
**2017tianchi competition**

**libsvmUL.scala————tansform the raw data to libsvm format**

**SVDUL.scala————using the svd to compute PCA which reduces the dimentions of data**

**KNN_longitude_latitude.py————knn only using the data of longitude and latitude**

**KNN_wifi_infos.py————knn using the data of wifi_infos but it is running so slowly**

**KNN_wifi_infos.scala————using spark to speed up the knn of using wifi infos, which can finish about several hours**

**MAP_UL.scala————using the ranking metric ——MAP to judge the sequence of wifis of shop and behavior orderd by the number of occurrences**

**TrueCheck.scala————using only the data of wifis which connection state is true**
