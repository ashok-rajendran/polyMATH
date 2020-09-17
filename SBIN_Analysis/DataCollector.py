import pandas as pd 
import yfinance as yf
from pandas_datareader import data
from pandas_datareader._utils import RemoteDataError
import matplotlib.pyplot as pit
import numpy as np
from datetime import datetime, date, timedelta
import os.path
from os import path
import sys

timestmp = str(datetime.now().strftime('%Y%m%d%H%M%S'))

stock = 'SBIN.NS'
tickerData = yf.Ticker(stock)
TgtFilesPath = "/histData/"
FileCheck = path.exists("/lastLoad/lastLoadFile.txt")
today = date.today()

if FileCheck is True:
    print("File Exist")
    lastLoad = open("/lastLoad/lastLoadFile.txt","r")
    content = lastLoad.read().split("|")
    start_date = content[2]
    end_date = str(today)
else:
    print("Not Exist, Hence Creating New File for last 7 days")
    start_date = str(today - timedelta(days=2))
    end_date = str(today)
    lastLoad = open("/lastLoad/lastLoadFile.txt","w+")
    lastLoad.write(stock + "|" + start_date + "|" + end_date)
    lastLoad.close()

if start_date == end_date:
    sys.exit("Exiting, Since Start date and End date is same!")


#create File Names
oneMinIntData = TgtFilesPath + "1MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
twoMinIntData = TgtFilesPath + "2MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
fiveMinIntData = TgtFilesPath + "5MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
fiftnMinIntData = TgtFilesPath + "15MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
thrtyMinIntData = TgtFilesPath + "30MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
sxtyMinIntData = TgtFilesPath + "60MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
nintyMinIntData = TgtFilesPath + "90MIN_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
oneHrIntData = TgtFilesPath + "1HR_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
oneDayIntData = TgtFilesPath + "1DAY_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
fivDaysIntData = TgtFilesPath + "5DAYS_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
onewkIntData = TgtFilesPath + "1WEEK_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
oneMonIntData = TgtFilesPath + "1MON_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"
threeMonsIntData = TgtFilesPath + "3MON_" + stock.upper() + "_" + start_date.replace('-','') + "_" + end_date.replace('-','') + "_" + timestmp + ".csv"

def collect_one_min_data(ticker):
    try:
        oneMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="1m")
        oneMinInt.to_csv(oneMinIntData)
    except RemoteDataError:
        print('No Data found for {t}'.format(t=ticker))

def collect_2m_to_1hr_data(ticker):
    try:
        twoMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="2m")
        twoMinInt.to_csv(twoMinIntData)
        fiveMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="5m")
        fiveMinInt.to_csv(fiveMinIntData)
        fiftnMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="15m")
        fiftnMinInt.to_csv(fiftnMinIntData)
        thrtyMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="30m")
        thrtyMinInt.to_csv(thrtyMinIntData)
        sxtyMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="60m")
        sxtyMinInt.to_csv(sxtyMinIntData)
        nintyMinInt = tickerData.history(period="max",start=start_date,end=end_date,interval="90m")
        nintyMinInt.to_csv(nintyMinIntData)
        oneHrInt = tickerData.history(period="max",start=start_date,end=end_date,interval="1h")
        oneHrInt.to_csv(oneHrIntData)
    except RemoteDataError:
        print('No Data found for {t}'.format(t=ticker))

def collect_1d_to_3mo_data(ticker):
    try:
        oneDayInt = tickerData.history(period="max",interval="1d")
        oneDayInt.to_csv(oneDayIntData)
        fivDaysInt = tickerData.history(period="max",interval="5d")
        fivDaysInt.to_csv(fivDaysIntData)
        onewkInt = tickerData.history(period="max",interval="1wk")
        onewkInt.to_csv(onewkIntData)
        oneMonInt = tickerData.history(period="max",interval="1mo")
        oneMonInt.to_csv(oneMonIntData)
        threeMonInt = tickerData.history(period="max",interval="3mo")
        threeMonInt.to_csv(threeMonsIntData)
    except RemoteDataError:
        print('No Data found for {t}'.format(t=ticker))


print("Collect Last 7 days SBIN Stock data of 1 Minute Timeframe!!")
collect_one_min_data(stock)

print("Collect Last 60 days SBIN Stock data of 2 Minutes to 1 Hour Timeframe!!")
collect_2m_to_1hr_data(stock)

print("Collect all Historic SBIN Stock data of 1 Day to 3 Months Timeframe!!")
collect_1d_to_3mo_data(stock)

#past_60_days =  str(today - timedelta(days=60))
