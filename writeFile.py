import pymongo as pm 
import datetime 
import time
import os

# In[2]:

def connect_db(dbhost,dbport,userid=None,passwd=None):
    try:
        # Connect to Mongo instance.
        if userid != None:
            return pm.MongoClient(username=userid,password=passwd, host=dbhost,port=dbport)
            #print('Mongo DB Connection established @: ',datetime.datetime.now())
        else:
            return pm.MongoClient(host=dbhost,port=dbport)
    except Exception as e:
        print("Unexpected error:", type(e), e)

# In[3]:

#make a connection to the database
connection = connect_db(dbhost='localhost',dbport=27017)

#attach to test database
db = connection.ticksdb

#attach to input_stocks collections.    
inputTickers = db.inputTickers
tickerData = db.tickerData  

path="/Users/kunkotha/Downloads/"
os.chdir(path)
# Write the header information first.
with open("OpFile.csv",'w') as f:
    datacur = tickerData.find({'tickerName': 'AAPL'}).sort('date',pm.DESCENDING).limit(1)
    for i in datacur:
        for j in i.keys():
            f.write(j+',')

# Write the latest day information for all the tickers.    
with open("OpFile.csv",'a') as f:
    f.write('\n')
    for validTickers in inputTickers.find({'extract':'Y'}).sort([('ticker',pm.ASCENDING)]):
         tickerName = validTickers['ticker']
         datacur = tickerData.find({'tickerName': tickerName}).sort('date',pm.DESCENDING).limit(1)
         for i in datacur:
            for j in i.values():
                f.write(str(j) + ',')
            f.write('\n')
