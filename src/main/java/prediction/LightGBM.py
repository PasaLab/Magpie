# -*- coding=utf-8 -*-
import os
import numpy as np
import csv    #需要加载numpy和csv两个包
import pandas as pd
import sys

jobType = sys.argv[1]
#jobType = "SQL"
path = sys.argv[0].split("java")[0] + "resources/trainData/{}".format(jobType)
print(path)
csv_file=open(path)    #打开文件
csv_reader_lines = csv.reader(csv_file)    #用csv.reader读文件
date_PyList=[]
for one_line in csv_reader_lines:
    date_PyList.append(one_line)    #逐行将读到的文件存入python的列表
date_ndarray = np.array(date_PyList)    #将python列表转化为ndarray

#根据作业和数据集类型 建立特征向量
if jobType == 'SQL':
    feature_names = ["Join", "Sort", "GroupBy","Hash", "BroadCast","Global","depth", "avgDegree","pathNum",
                 "minPath","datasize","item_avg","item_var","web_sales_avg","web_sales_var","date_dim_avg",
                 "date_dim_var","store_sales_avg","store_sales_var","catalog_sales_avg","catalog_sales_var",
                "customer_demographics_avg","customer_demographics_var","call_center_avg","call_center_var",
                 "catalog_returns_avg","catalog_returns_var","customer_avg","customer_var","customer_address_avg",
                 "customer_address_var","store_avg","store_var","store_returns_avg", "store_returns_var",
                 "TM","slot","NM","MM","parallelism"]
if jobType == 'ML':
    feature_names = ["Map", "Reduce", "Iterate","Hash", "BroadCast","Global","depth", "avgDegree","pathNum",
                 "minPath","datasize","TM","slot","NM","MM","parallelism"]
if jobType == 'Graph':
    feature_names = ["Map", "Reduce", "Iterate","Hash", "BroadCast","Global","depth", "avgDegree","pathNum",
                 "minPath","datasize","nodeNum","edgeNum","degree","cycle","TM","slot","NM","MM","parallelism"]
if jobType == 'Stream':
    feature_names = ["Join", "Sort", "GroupBy","Hash", "BroadCast","Global","depth", "avgDegree","pathNum",
                 "minPath","throughput","recordSize","TM","slot","NM","MM","parallelism"]

feature = np.array(feature_names)
#后5个是预测参数Y
featureNum = len(feature_names)-5

date_x = date_ndarray[:,0:featureNum]
date_y = date_ndarray[:,featureNum:featureNum+5]
feature_x = feature[0:featureNum]
feature_y = feature[featureNum:featureNum+5]



if __name__ == '__main__':
    #加载LightGBM库
    from sklearn.model_selection import train_test_split
    from sklearn.multioutput import MultiOutputRegressor
    import lightgbm as lgb

    #读取特征集
    X = pd.DataFrame(date_x).astype(np.float64)
    X.columns = feature_x

    Y = pd.DataFrame(date_y).astype(np.float64)
    Y.columns = feature_y

    #-------LightGBM回归模型
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.30, random_state = 0)

    lgbm = MultiOutputRegressor(lgb.LGBMRegressor(num_leaves=31,
                            learning_rate=0.05,
                            n_estimators=20))
    lgbm.fit(X, Y)
    
    # #模型评估  Mean Absolute Error (MAE)
    # lgbm.fit(X_train, Y_train)
    # Y_pred = lgbm.predict(X_test
    # print(Y_pred)
    
    # from sklearn.metrics import mean_absolute_error
    # mae = mean_absolute_error(Y_test, Y_pred)
    # err = " MAE: " + str(round(mae, 2))
    # print("Light Gradient Boosting Machine Model Performance: ", err)    
    
    #获取输入特征向量
    input = str(sys.argv[2]).split(",")
    #input = "6.0,2.0,8.0,2.0,6.0,1.0,30.0,1.0,8.0,12.0,1.0,1.0,0.0,0.0,0.0,2150.614842454395,7771096.627262046,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0".split(",")
    featureValue = [list(map(float, input))]
    #模型预测
    Y_pred = lgbm.predict(featureValue)[0]

    #添加参数单位、修改小数点位数
    result = str(int(Y_pred[0]))+"g"
    for  i in range(1, len(Y_pred)):
        if Y_pred[i] < 1 :
            result = result+","+ str('%.2f' % Y_pred[i])
        else:
            result = result + "," + str(int(Y_pred[i]))
    print(result)