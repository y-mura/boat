
# coding: utf-8

# In[ ]:

import os
from urllib.request import urlretrieve
from urllib.request import urlopen
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import re
import time
import numpy as np
import pandas as pd
from pandas import Series,DataFrame


# In[18]:

def GetInternalLinks(bsObj, includeUrl):
    includeUrl = urlparse(includeUrl).scheme+"://"+urlparse(includeUrl).netloc
    internalLinks = []
    #Finds all links that begin with a "/"
    for link in bsObj.findAll("a", {"class":"result"}):
        if link.attrs['href'] is not None:
            if link.attrs['href'] not in internalLinks:
                if(link.attrs['href'].startswith("/")):
                    internalLinks.append(includeUrl+link.attrs['href'])
                else:
                    internalLinks.append(link.attrs['href'])
    return internalLinks


# In[19]:

def GetBS(url):
    html = urlopen(url)
    bs = BeautifulSoup(html,"lxml")
    return bs


# In[20]:

def GetResult(bsObj):
    result = bsObj.find("div", {"class":"multiColumn2"}).find("table", {"class":"sTable"}).findAll("td")
    df = pd.DataFrame(np.arange(24).reshape((6,4)), columns = ["Rank","Lane","Name","Time"])
    for var in range(0, 6):
        i = var * 4
        se = pd.Series([result[i].text,result[i+1].text,result[i+2].text,result[i+3].text],index = ["Rank","Lane","Name","Time"])
        df.ix[var] = se
    return df


# In[33]:

def GetPlayerList(bsObj):
    tables = bsObj.find("div", {"class":"multiColumn6"}).findAll("table")
    # 氏名、級別、レーン、平均スタート時間、全国勝率、全国2連対率、当地勝率、当地2連対率、ボート、モーター
    df = pd.DataFrame(np.arange(66).reshape((6,11)), columns = ["Id","Name","Class","Lane","AvgSt","AllWinRate","All2renRate","PlaceWinRate","Place2renRate","Motor","Boat"])
    i = 0
    for table in tables:
        player = table.findAll("th")
        Id = player[2].text
        player = table.findAll("td")
        Name = player[0].text
        Class = player[1].text
        AvgSt = player[8].text
        AllWinRate = player[9].text
        All2renRate = player[10].text
        PlaceWinRate = player[11].text
        Place2renRate = player[12].text
        Motor = player[13].text
        Boat = player[15].text
        se = pd.Series([Id,Name,Class,i+1,AvgSt,AllWinRate,All2renRate,PlaceWinRate,Place2renRate,Motor,Boat],index = ["Id","Name","Class","Lane","AvgSt","AllWinRate","All2renRate","PlaceWinRate","Place2renRate","Motor","Boat"])
        
        df.ix[i] = se
        i += 1
    return df


# In[46]:

def ScraypingProcess(url):
    link = url.replace('result','program')
    bsObj = GetBS(url)
    result = GetResult(bsObj)
    
    bsObj = GetBS(link)
    playerList = GetPlayerList(bsObj)
    
    p = re.compile(r'\d+')
    
    time.sleep(5)
    result.Lane = result.Lane.convert_objects(convert_numeric=True).fillna(-1).astype(np.int)
    result.Rank = result.Rank.apply(int_value)
    result.Rank = result.Rank.convert_objects(convert_numeric=True).fillna(6).astype(np.int)
    
    result = pd.merge(playerList, result, how="inner", on=["Name","Lane"])
    day = link[48:56]
    jyo = link[61:63]
    rnd = link[68:70]
    result["Day"] = day
    result["Jyo"] = jyo
    result["Round"] = rnd
    
    return result


# In[23]:

def AccessYearUnit(year):
    month = 1
    yResult = GetDatesOfMonth(year, month)
    month += 1
    
    while month < 13:
        temp = GetDatesOfMonth(year, month)
        yResult += temp
        month += 1
    return yResult


# In[24]:

def int_value(x):
    try:
        return int(x)
    except ValueError:
        return np.nan


# In[25]:

import datetime
import calendar

def GetDatesOfMonth(year, month):
    month_days = [i+1 for i in range(calendar.monthrange(year, month)[1])]

    output_date_list = []
    if month < 10:
        month = "0" + str(month)
    str_target_year_month = str(year) + str(month)

    for each_day in month_days:
        if each_day < 10:
            each_day = "0" + str(each_day)
        output_date_list.append(str(str_target_year_month) + str(each_day))
    
    return output_date_list


# In[26]:

import sqlite3
import pandas.io.sql as psql

def CreateSqliteTable(data):
    with sqlite3.connect("tmp.db") as conn:
      #これでINSERTされる
        psql.to_sql( data, 'foo', conn, if_exists='replace', index=False)
    return


# In[44]:

def GetSqliteTable():
# 中身確認
    with sqlite3.connect("tmp.db") as conn:
        sql = "select * from foo"
        db = psql.read_sql(sql, conn)
    return db


# In[28]:

def AccessMonthUnit(year, month):
    days = GetDatesOfMonth(year, month)
    return days


# In[29]:

def AccessDayUnit(day):
    baseUrl = "http://app.boatrace.jp/race"
    accessUrl = "http://app.boatrace.jp/race/?day=yyyymmdd"
    
    dayUrl = accessUrl.replace('yyyymmdd', str(day))
    bsObj = GetBS(dayUrl)
    linkList = GetInternalLinks(bsObj,baseUrl)
    i = 1
    
    for link in linkList:
        print (link)
        
        if i == 1:
            dResult = ScraypingProcess(link)
        else:
            temp = ScraypingProcess(link)
            dResult = pd.concat([dResult, temp], ignore_index=True)
        i += 1
    return dResult


# In[48]:
#ここから
#初回のみ、DB作るために2015/12/31のデータを取得する
#2回目以降はGetSqliteTableでデータを取得し、DataFrameにconcatして、再度上書きモードで保存する
#なおSqlite3は組み込みのため、インストールの必要はなく、DBファイルが相対ディレクトリに作成される
#data = AccessDayUnit(20151231)
#CreateSqliteTable(data)

#dbファイルがある場合はここから、年と月を切り替えて使用できる
#年単位のメソッドもあるが、タイムアウトすると思う
dbData = GetSqliteTable()
days = GetDatesOfMonth(2016,1)

for day in days:
    temp = AccessDayUnit(day)
    result = pd.concat([dbData, temp], ignore_index=True)
CreateSqliteTable(result)
# In[46]:

#あとすること
#・機械学習


# In[ ]:



