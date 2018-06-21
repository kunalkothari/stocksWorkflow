import pymongo as pm 
import pandas_datareader as pdr 
import matplotlib.pyplot as plt
import datetime 
import pandas as pd 
import time
import random
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

# ### Fetch Data from Yahoo and upload to MongoDB

# In[4]:

def getNextVal(sequenceName): 
    seqDoc = db.counters.find_one_and_update({'_id':sequenceName},{'$inc': {'sequenceValue':1}},projection={'sequenceValue':True,'_id':False},return_document=pm.ReturnDocument.AFTER)
    return seqDoc

# In[5]:
#used the enumerate to get the iteration number for the Ticker picked from the list and used later on to sleep before making another call
#to yahoo api to get the ticker information for the next Ticker. 

for i,ticker in enumerate(inputTickers.find({'extract':'Y'}).sort([("ticker",pm.ASCENDING)])):
    startDate = datetime.datetime(2004, 1, 1).date()
    endDate = datetime.date.today()
    tickerName= ticker['ticker']
    countDate = tickerData.find({'tickerName':tickerName}).count()
    print("Count for:",tickerName,"is",countDate)
    #time.sleep(1)
    if countDate != 0:
        #print("Inside countDate !=0")
        latestData = tickerData.find({'tickerName':tickerName}).sort([("date",pm.DESCENDING)]).limit(1)
        for latestRecord in latestData:
            #latestDate = latestRecord['date']
            latestDate = datetime.datetime.strptime(latestRecord['date'], '%Y-%m-%d')
            #print("value for latestDate is:",latestDate)            
        #print("value for latestDate.date() is:",latestDate.date(),"and current date is:",datetime.datetime.today().date())
        if latestDate.date() == datetime.datetime.today().date():
            #startDate = datetime.datetime.today().date()
            continue  #if we have the latest data for ticker then move on to next ticker
        elif latestDate.date() < datetime.datetime.today().date():    
            startDate = latestDate + datetime.timedelta(days=1) # increment latestDate by 1 day to get the next date value.
            #print("ive figured we have data in mongo and the startDate ive to pick is:" , startDate,"in mongo it is",latestDate)          
    else:
        #print("inside else of countDate !=0 assigning start date to 2004")
        startDate = datetime.datetime(2004, 1, 1)
        
    #ensure that it is past 3pm to extract the latest end of the day data. 
    '''now = datetime.datetime.now()
    today3pm = now.replace(hour=15, minute=0, second=0, microsecond=0)
    if now < today3pm:
        print("Its not 3pm yet so skipping the extract")
        continue
    else:'''    
    print("Fetching data for:",tickerName,"with Start Date:",startDate,"and End Date:",endDate)
    try:
        df = pdr.get_data_yahoo(tickerName, start=startDate, end=endDate)
        #pdr.data.DataReader('PG', data_source='yahoo', start='1995-1-1')
    except Exception as e:
        print("Unexpected error:", type(e), e)
        db.errorTicker.insert_one({'errorDate': datetime.datetime.today(), 'errorTicker': tickerName, 'errorProcess':'Get data from Yahoo failed'})
        continue

    #insert into the final collection tickerData 
    #for Date,Open,High,Low, Close, Adj_Close,Volume,Stock in daily_data.find({'Stock':tickerName}): 
    df.reset_index(level=0, inplace=True)
    for ticks in df.itertuples():
        #date = datetime.datetime.strptime(str(ticks[1])[:10], '%Y-%m-%d')
        newVal = getNextVal('sno')
        date = str(ticks[1])[:10]
        open = round(ticks[2],2)
        high = round(ticks[3],2)
        low = round(ticks[4],2)
        close = round(ticks[5],2)
        adj_close = round(ticks[6],2)
        volume = ticks[7]
        record = {'_id' : int(newVal['sequenceValue']),'tickerName' : tickerName, 'date': date, 'open': open, 'high':high, 'low':low, 'close':close, 'adj_close':adj_close, 'volume':volume}
        try:
            tickerData.insert_one(record)
        except Exception as e:
            print("Unexpected error:", type(e), e)
            db.errorTicker.insert_one({'errorDate': datetime.datetime.today(), 'errorTicker': tickerName,'errorProcess':'Error inserting into tickerData collection'})            
            continue

    print("Data for ticker",tickerName, "inserted successfully!")            
    countPost = tickerData.find({'tickerName':tickerName}).count()
    print("Count for:",tickerName,"is",countPost)    

    time.sleep(random.randint(1,5))

# ### Calculate the Moving and Exponential Averages and save to MongoDB

for validTickers in inputTickers.find({'extract':'Y'}).sort([("ticker",pm.ASCENDING)]):
    tickerName = validTickers['ticker']
    print("Working on Averages Generation for ticker:", tickerName)
    datacur = tickerData.find({'tickerName': tickerName}).sort('date',pm.ASCENDING)
    # Expand the cursor and construct the DataFrame
    if (datacur.count() > 0):
        data = pd.DataFrame(list(datacur))
        close = data['close']
        volume = data['volume']
        data1 = data
        sma10 = close.rolling(window=10)
        data1['sma_10days']=sma10.mean()
        sma20 = close.rolling(window=20)
        data1['sma_20days']=sma20.mean()
        sma50 = close.rolling(window=50)
        data1['sma_50days']=sma50.mean()
        sma100 = close.rolling(window=100)
        data1['sma_100days']=sma100.mean()
        sma200 = close.rolling(window=200)
        data1['sma_200days']=sma200.mean()
        
        smavol5 = volume.rolling(window=5)
        data1['sma_vol_5days']=smavol5.mean()
        smavol10 = volume.rolling(window=10)
        data1['sma_vol_10days']=smavol10.mean()

        data1['ema_3days']=close.ewm(span=3,min_periods=3).mean()
        data1['ema_5days']=close.ewm(span=5,min_periods=5).mean()
        data1['ema_8days']=close.ewm(span=8,min_periods=8).mean()
        data1['ema_10days']=close.ewm(span=10,min_periods=10).mean()
        data1['ema_20days']=close.ewm(span=20,min_periods=20).mean()
        data1['ema_34days']=close.ewm(span=34,min_periods=34).mean()
        data1['ema_50days']=close.ewm(span=50,min_periods=50).mean()
        data1['ema_100days']=close.ewm(span=100,min_periods=100).mean()
        data1['ema_200days']=close.ewm(span=200,min_periods=200).mean()

        #iterate over the dataframe
        for record in data1.itertuples():
            #1st column of the dataframe is the Object Id 
            #oid = ObjectId(record[1]) 
            oid = record[1]
            #update the newly calculated sma and ema columns into the ticker collection
            #update happens based on ObjectId of the row fetched originally, 
            #data converted to float after rounding to 2 decimals. 
            db.tickerData.update_one({'_id':oid},
                         {"$set":{"sma_10days":float(round(record.sma_10days,2)),
                                  "sma_20days":float(round(record.sma_20days,2)),
                                  "sma_50days":float(round(record.sma_50days,2)),
                                  "sma_100days":float(round(record.sma_100days,2)),
                                  "sma_200days":float(round(record.sma_200days,2)),
                                  "ema_3days":float(round(record.ema_3days,2)),
                                  "ema_5days":float(round(record.ema_5days,2)),
                                  "ema_8days":float(round(record.ema_8days,2)),
                                  "ema_10days":float(round(record.ema_10days,2)),
                                  "ema_20days":float(round(record.ema_20days,2)),
                                  "ema_34days":float(round(record.ema_34days,2)),
                                  "ema_50days":float(round(record.ema_50days,2)),
                                  "ema_100days":float(round(record.ema_100days,2)),
                                  "ema_200days":float(round(record.ema_200days,2)),
                                  "sma_vol_5days": round(record.sma_vol_5days,0), 
                                  "sma_vol_10days": round(record.sma_vol_10days,0)}}, upsert=False)
    else:
        print("Skipping ticker:", tickerName,"as no data present!")

# ### Determine the SELL or BUY signals for various studies

def crossovers(study,faster,slower,fasterValue,slowerValue):
    fieldName = study.lower() +'_'+ str(faster) +'x'+ str(slower)+'_cross'
    if fasterValue >= slowerValue:
        return (fieldName,'BUY')
    else:
        return (fieldName,'SELL')

# In[8]:


for validTickers in inputTickers.find({'extract':'Y'}).sort([("ticker",pm.ASCENDING)]):
    tickerName = validTickers['ticker']
    print("Working on Signals Generation for ticker:", tickerName)
    cursor = tickerData.find({'tickerName': tickerName}).sort('date',pm.ASCENDING)
    #Loop through the cursor and update the studies columns
    for data in cursor:
        col3x5,pos3x5 = crossovers('ema',3,5,data['ema_3days'],data['ema_5days'])
        col8x34,pos8x34 = crossovers('ema',8,34,data['ema_8days'],data['ema_34days'])
        col10x20,pos10x20 = crossovers('ema',10,20,data['ema_10days'],data['ema_20days'])
        col20x50,pos20x50 = crossovers('ema',20,50,data['ema_20days'],data['ema_50days'])
        col50x100,pos50x100 = crossovers('ema',50,100,data['ema_50days'],data['ema_100days'])
        col50x200,pos50x200 = crossovers('ema',50,200,data['ema_50days'],data['ema_200days'])
        col100x200,pos100x200 = crossovers('ema',100,200,data['ema_100days'],data['ema_200days'])
        
        scol10x20,spos10x20 = crossovers('sma',10,20,data['sma_10days'],data['sma_20days'])
        scol20x50,spos20x50 = crossovers('sma',20,50,data['sma_20days'],data['sma_50days'])
        scol50x100,spos50x100 = crossovers('sma',50,100,data['sma_50days'],data['sma_100days'])
        scol50x200,spos50x200 = crossovers('sma',50,200,data['sma_50days'],data['sma_200days'])
        scol100x200,spos100x200 = crossovers('sma',100,200,data['sma_100days'],data['sma_200days'])
        
        tickerData.update_one({'_id':data['_id']}, {"$set": { col3x5:pos3x5,
                                                                  col8x34:pos8x34,
                                                                  col10x20:pos10x20, 
                                                                  col20x50:pos20x50, 
                                                                  col50x100:pos50x100, 
                                                                  col50x200:pos50x200, 
                                                                  col100x200:pos100x200, 
                                                                  scol10x20:spos10x20, 
                                                                  scol20x50:spos20x50, 
                                                                  scol50x100:spos50x100, 
                                                                  scol50x200:spos50x200, 
                                                                  scol100x200:spos100x200}})


