import os
import pandas as pd

Height_WID = [500,1000,1500,2000,2500,3000,3500,4000,4500,5000,
					5500,6000,6500,7000,7500,8000,8500,9000,9500,10000]
def str_path(path,string):
    list_name = []
    for file in os.listdir(path):
        file_path = os.path.join(path, file)  
        if os.path.isdir(file_path):  
            str_path(file_path,string)  
        else:  
            list_name.append(file_path)
    return [i for i in list_name if i.find(string)!= -1]

def data_from_txt(filepath):
    data = pd.read_csv(filepath, sep=' ', names=['height','horizontald','horizontalv'])
    station = data.iloc[1,0]
    latitude = data.iloc[1,1]
    longitude = data.iloc[1,2]
#     print 'station:',station,'longitude:',longitude,'latitude:',latitude
    
    
    newdata = data[3:-1]
    newdata = newdata.apply(pd.to_numeric, errors='coerce')
    newdata = newdata.dropna()
#     print newdata
#     newdata.replace('/////',9999,inplace = True)
    
#     # remove row value 9999
#     newdata[newdata.horizontald.isin([9999])]
#     test = list(newdata.horizontald)
#     while 9999 in test:
#         test.remove(9999)
#     newdata = newdata[newdata.horizontald.isin(test)]
#     print newdata['horizontald'].str.isdigit()
    dic = {}
    height = newdata.iloc[:,0].tolist()
    
    for i,item in enumerate(height):
        for j in Height_WID:
            if abs(int(item)-j)<500:
                if j in dic.keys():
                    if abs(int(item)-j) < abs(int(height[dic[j]])-j):
                        dic[j] = i
                else:
                    dic[j] = i
    a = []
    b = []
    for i in dic:
        a.append([i,dic[i]])
        b.append(dic[i])
    newdata = newdata.copy()
    for i in a:
        newdata.iloc[i[1],0] = i[0]
    fdata = newdata.iloc[b].sort_values(by='height').copy()
    dic1 = {}
    for index,row in fdata.iterrows():
        dic1[row['height']] = {'horizontald':row['horizontald'],'horizontalv':row['horizontalv']}
    return dic1,station,float(latitude),float(longitude)


import sqlite3
    
if __name__ == '__main__':
    path = 'C:/20180914/'
    file_list = str_path(path,'OOBS')
    
    conn = sqlite3.connect('combination.db')
    cursor  = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'"% 'OOBS')
    if len(cursor.fetchall()) == 0:
        cursor.execute('create table OOBS(time_2 varchar(10),station_code varchar(10),height int,horizontalv float,horizontald float,latitude float,longitude float)')
        conn.commit()
    j = 0
    for i in file_list:
        print j
        time = i.split('_')[-1].split('.')[0][0:10]
        dic,station,latitude,longitude = data_from_txt(i)
#         print dic
        for key in dic.keys():
            cursor.execute("insert into OOBS(time_2,station_code,height,horizontalv,horizontald,latitude,longitude) values('%s','%s','%d','%f','%f','%f','%f')"%(time,station,key,dic[key]['horizontalv'],dic[key]['horizontald'],latitude,longitude))
            conn.commit()
        j+=1
        
    cursor.close()
    conn.close()