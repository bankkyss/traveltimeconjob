import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from pytz import timezone
#### config 
thai = timezone('Asia/Bangkok')
hostname="161.200.116.30:31212"
dbname="iticrealtime"
uname="apiitic_1"
pwd="iticprob1"

def getdata(endtime):
    starttime=endtime-timedelta(minutes=20)
    if starttime.month==endtime.month:
        tablename='prob_'+endtime.strftime("%Y_%m")
        print(tablename)
        tablename='realtimedataproball'
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))
        query='''SELECT * FROM '''+tablename+''' where timestamp between %(start)s and %(end)s and  speed >0 and projectionlat between 13.696316544431221 and 13.761531506234158 and projectionlon between 100.50252356649676 and 100.6244036119347 and errorprojection <30 ;'''
        data=pd.read_sql(query,engine,params={'start':starttime.strftime("%Y-%m-%d %H:%M:%S"),'end':endtime.strftime("%Y-%m-%d %H:%M:%S")}).drop_duplicates()
        data.timestamp=pd.to_datetime(data.timestamp)

    else:
        data=[]
        for timeint in [starttime,endtime]:
            tablename='prob_'+timeint.strftime("%Y_%m")
            print(tablename)
            tablename='realtimedataproball'
            engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))
            query='''SELECT * FROM '''+tablename+''' where timestamp between %(start)s and %(end)s and  speed >=0 and projectionlat between 13.696316544431221 and 13.761531506234158 and projectionlon between 100.50252356649676 and 100.6244036119347 and errorprojection <30 ;'''
            data.append(pd.read_sql(query,engine,params={'start':starttime.strftime("%Y-%m-%d %H:%M:%S"),'end':endtime.strftime("%Y-%m-%d %H:%M:%S")}).drop_duplicates())
        data=pd.concat(data)
        data.timestamp=pd.to_datetime(data.timestamp)
    return(data)        
#print(len(getdata(datetime.now(thai))))
print(len(getdata(datetime(2022,2,1,0,0))))