import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime,timedelta
from pytz import timezone
import requests
import math
from bs4 import BeautifulSoup
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
# date_time=datetime.now()
# date_time=(date_time-timedelta(minutes=date_time.minute%10,seconds=date_time.second)).replace(second=00,microsecond=00)  
# print(date_time)
#print(len(getdata(datetime(2022,2,1,0,0))))
def getraindata(ID):
    try:
        URL = "https://weather.bangkok.go.th/rain/StationDetail?id="+str(ID)
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        tags=soup.find_all("li", class_="list-group-item")
        for tag in tags:
            data=[]
            for cont in [str(i.string) for i  in tag]:
                if type(cont)==str:
                    data.append(cont.replace("\r","").replace("\n","").replace(" ",""))
            if 'ฝนสะสม15นาที' in data:
                raincount=(float(data[1][:-3]))
            if 'เวลาข้อมูล' in data:
                strtime=(data[1][:-2])
    except:
        raincount=float('nan')
    return raincount
station={'KhlongToeiOffice':30,
 'PhraKhanongPumpingStation':81,
 'BenchasiriPark':95,
 'Rama4PumpingStation':80,
 'PathumWanOffice':8,
 'Sawasdee Pier':3}

def rainstation(timestat):
    try:
        data={'time':timestat}
        for po in station:
            if po =='Sawasdee Pier':
                data[po]=float('nan')
            else:
                data[po]=getraindata(station[po])
        print(data)
        print([data[i] for i in station if not(math.isnan(data[i])) ])
        rainindex=max([data[i] for i in station if not(math.isnan(data[i])) ])
        #data=pd.DataFrame([data])
        #engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db='raindata', user=uname, pw=pwd))
        #data.to_sql('rainstation', engine, index=False,if_exists="append")
        return rainindex
    except:
        return float('nan')

print(rainstation(12))