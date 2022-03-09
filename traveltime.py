import joblib
import shapely
import geohash
import osmnx as ox
import pandas as pd
import networkx as nx
import multiprocessing
import numpy as np
import time
import os
from pytz import timezone
dirname = os.path.dirname(__file__)
from datetime import datetime,timedelta
from sqlalchemy import create_engine
from shapely.geometry import Point, LineString
from geopy.distance import great_circle

#### config 
thai = timezone('Asia/Bangkok')
hostname=os.getenv('hostname', "161.200.116.30:31212")
dbname='iticprob'
uname="apiitic_1"
pwd="iticprob1"

#### loaddata
datahash,G,nodes,edges=joblib.load(os.path.join(dirname,'fortraveltime.p'))
keyway=joblib.load(os.path.join(dirname,'wayrenamerama4.p'))


def tranfromdatase(lat,lon,bea):
    try:
        dist=float('inf')
        hashid=geohash.encode(lat,lon, precision=6)
        if hashid in datahash.keys():
            for road in datahash[hashid]:
                if abs(road['bearing']-bea)<90 or abs(bea-road['bearing'])<90 or road['oneway']:
                    ss=great_circle((road['startnode'][1],road['startnode'][0]),(lat,lon)).meters+great_circle((road['endnode'][1],road['endnode'][0]),(lat,lon)).meters-great_circle((road['startnode'][1],road['startnode'][0]),(road['endnode'][1],road['endnode'][0])).meters
                    if dist>ss:
                        dataroad=road
                        dist=ss
            shply_line=LineString([Point(dataroad['startnode'][0],dataroad['startnode'][1]),Point(dataroad['endnode'][0],dataroad['endnode'][1])])
            distant=shply_line.project(shapely.geometry.Point(lon,lat))
            projecttion=shply_line.interpolate(distant)
            error=(great_circle((projecttion.y,projecttion.x),(lat,lon))).meters
            if error>100:
                return {'wayid':float('nan'),'projectionlat':float('nan'),'projectionlon':float('nan'),'errorprojection':float('nan')}
            return {'wayid':dataroad['name'],'projectionlat':projecttion.y,'projectionlon':projecttion.x,'errorprojection':error}
    except:
        return {'wayid':float('nan'),'projectionlat':float('nan'),'projectionlon':float('nan'),'errorprojection':float('nan')}

def cheagelist(way):
    if type(way)==list:
        return ','.join([str(elem) for elem in way])
    else:
        return str(way)

def caltraveltime(stlat,stlon,stbr,stspeed,sttime,splat,splon,spbr,spspeed,sptime):
    #find v(startpoint)  u(stoppoint)
    stway=tranfromdatase(stlat,stlon,stbr)['wayid']
    if len(edges[edges.osmid==str(stway)])==1:
        startnode=edges[edges.osmid==str(stway)].index[0][1]
    else:
        dist=float('inf')
        for i in edges[edges.osmid==str(stway)].index:
            stdis1=great_circle(tuple(nodes[nodes.index==i[1]][['y','x']].values.tolist()[0]),(stlat,stlon)).meters
            stdis2=great_circle(tuple(nodes[nodes.index==i[0]][['y','x']].values.tolist()[0]),(stlat,stlon)).meters
            stdisno=great_circle(tuple(nodes[nodes.index==i[0]][['y','x']].values.tolist()[0]),tuple(nodes[nodes.index==i[1]][['y','x']].values.tolist()[0])).meters
            stnodedos=stdis1+stdis2-stdisno
            if stnodedos<dist:
                dist=stnodedos
                startnode=i[1]
    spway=tranfromdatase(splat,splon,spbr)['wayid']
    if len(edges[edges.osmid==str(spway)])==1:
        stopnode=edges[edges.osmid==str(spway)].index[0][0]
    else:
        dist=float('inf')
        for i in edges[edges.osmid==str(spway)].index:
            spdis1=great_circle(tuple(nodes[nodes.index==i[0]][['y','x']].values.tolist()[0]),(splat,splon)).meters
            spdis2=great_circle(tuple(nodes[nodes.index==i[1]][['y','x']].values.tolist()[0]),(splat,splon)).meters
            spdisno=great_circle(tuple(nodes[nodes.index==i[0]][['y','x']].values.tolist()[0]),tuple(nodes[nodes.index==i[1]][['y','x']].values.tolist()[0])).meters
            spnodedos=spdis1+spdis2-spdisno
            if spnodedos<dist:
                dist=spnodedos
                stopnode=i[0]
    #find distant u->v
    route = nx.shortest_path(G=G, source=startnode, target=stopnode, weight='length')
    length = int(sum(ox.utils_graph.get_route_edge_attributes(G, route, "length")))
    if length <50:
        return
    waydata = ox.utils_graph.get_route_edge_attributes(G, route)
    #find distant st->u v->sp
    stdis=great_circle(tuple(nodes[nodes.index==startnode][['y','x']].values.tolist()[0]),(stlat,stlon)).meters
    spdis=great_circle(tuple(nodes[nodes.index==stopnode][['y','x']].values.tolist()[0]),(splat,splon)).meters
    #distant all route
    distantroute=length+stdis+spdis
    #calavspeed
    timereal=(sptime-sttime).total_seconds()
    if timereal<60:
        return
    averagespeed=distantroute/timereal
    if averagespeed>max(stspeed,spspeed)/18*5+6:
        return
    #caldistant in wayid
    locway={}
    locway[cheagelist(stway)]=stdis
    for way in waydata:
        osmid=cheagelist(way['osmid'])
        if osmid not in locway.keys():
            locway[osmid]=way['length']
        else:
            locway[osmid]+=way['length']
    if cheagelist(spway) not in locway.keys():
        locway[cheagelist(spway)]=spdis
    else:
        locway[cheagelist(spway)]+=spdis
    speed={}
    for key in locway:
        speed[key]=(averagespeed*18/5)
    datareturn=[]
    for i in speed:
        if len(i.split(','))==1:
            datareturn.append({'startime':sttime,'endtime':sptime,'wayid':i,'speedaver':speed[i],'distant':locway[i]})
        else:
            for j in i.split(','):
                datareturn.append({'startime':sttime,'endtime':sptime,'wayid':j,'speedaver':speed[i],'distant':locway[i]})
    return datareturn

def getdata(endtime):
    starttime=endtime-timedelta(minutes=20)
    #print(starttime.month==endtime.month)
    if starttime.month==endtime.month:
        tablename='prob_'+endtime.strftime("%Y_%m")
        #print('case1',tablename)
        #tablename='realtimedataproball'
        engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))
        query='''SELECT * FROM '''+tablename+''' where timestamp between %(start)s and %(end)s and  speed >=0 and projectionlat between 13.696316544431221 and 13.761531506234158 and projectionlon between 100.50252356649676 and 100.6244036119347 and errorprojection <30 ;'''
        data=pd.read_sql(query,engine,params={'start':starttime.strftime("%Y-%m-%d %H:%M:%S"),'end':endtime.strftime("%Y-%m-%d %H:%M:%S")}).drop_duplicates()
        data.timestamp=pd.to_datetime(data.timestamp)
    else:
        data=[]
        for timeint in [starttime,endtime]:
            tablename='prob_'+timeint.strftime("%Y_%m")
            print(tablename)
            #tablename='realtimedataproball'
            engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=dbname, user=uname, pw=pwd))
            query='''SELECT * FROM '''+tablename+''' where timestamp between %(start)s and %(end)s and  speed >0 and projectionlat between 13.696316544431221 and 13.761531506234158 and projectionlon between 100.50252356649676 and 100.6244036119347 and errorprojection <30 ;'''
            data.append(pd.read_sql(query,engine,params={'start':starttime.strftime("%Y-%m-%d %H:%M:%S"),'end':endtime.strftime("%Y-%m-%d %H:%M:%S")}).drop_duplicates())
        data=pd.concat(data)
        data.timestamp=pd.to_datetime(data.timestamp)
    #print(len(data))
    return(data)   

def processsplit(data):
    traveltime=[]
    for carid in data.vehicleid.unique():
        carloc=data[data.vehicleid==carid].sort_values(['timestamp'])
        if len(carloc)==1:
            carloc=carloc.iloc[0]
            if carloc['speed']>0:
                a=tranfromdatase(carloc['projectionlat'],carloc['projectionlon'],carloc['direction'])
                dis=(float(edges[edges.osmid==str(a['wayid'])][['length']].sum()))
                if dis>0:
                    if type(a['wayid']) is list:
                        for subwaydata in a['wayid']:
                            traveltime.append({'startime':carloc['timestamp'],'endtime':carloc['timestamp'],'wayid':subwaydata,'speedaver':carloc['speed']*5/18,'distant':dis/10})
                    else:
                        traveltime.append({'startime':carloc['timestamp'],'endtime':carloc['timestamp'],'wayid':a['wayid'],'speedaver':carloc['speed']*5/18,'distant':dis/10})
        else:
            isfirst=True
            for i,row in carloc.iterrows():
                if isfirst:
                    stlat=row['projectionlat']
                    stlon=row['projectionlon']
                    stbr=row['direction']
                    stspeed=row['speed']
                    sttime=row['timestamp']
                    isfirst=False
                else:
                    splat=row['projectionlat']
                    splon=row['projectionlon']
                    spbr=row['direction']
                    spspeed=row['speed']
                    sptime=row['timestamp']
                    if (sptime-sttime).total_seconds()<1000:
                        try:
                            listdata=caltraveltime(stlat,stlon,stbr,stspeed,sttime,splat,splon,spbr,spspeed,sptime)
                            traveltime+=listdata
                        except:
                            pass
                    else:
                        a=tranfromdatase(stlat,stlon,stbr)
                        dis=(float(edges[edges.osmid==str(a['wayid'])][['length']].sum()))
                        if dis>0:
                            if type(a['wayid']) is list:
                                for subwaydata in a['wayid']:
                                    traveltime.append({'startime':sttime,'endtime':sptime,'wayid':subwaydata,'speedaver':stspeed,'distant':dis/10})
                            else:
                                traveltime.append({'startime':sttime,'endtime':sptime,'wayid':a['wayid'],'speedaver':stspeed,'distant':dis/10})
                    stlat=splat
                    stlon=splon
                    stbr=stbr
                    stspeed=spspeed
                    sttime=sptime
    caldata2=pd.DataFrame(traveltime).dropna()
    caldata2.wayid=caldata2.wayid.astype(dtype = int, errors = 'ignore')
    return(caldata2)

def process(data,core=4):
    carid=np.array_split(data.vehicleid.unique(),core)
    data_split =[data[data['vehicleid'].isin(i)] for i in carid]
    pool = multiprocessing.Pool(processes=core)
    data = pd.concat(pool.map(processsplit, data_split))
    #data = pool.map(processsplit, data_split)
    pool.close()
    pool.join()
    return data

def process10min(date_time,core=4):
    data=getdata(date_time)
    datatime=processsplit(data)
    #datatime=process(data,core)
    datanow={'time':date_time}
    for i in keyway.keys():
        waydata=datatime[datatime.wayid.isin(keyway[i])]
        if len(waydata)!=0:
            try:
                datanow[i]=sum(waydata.speedaver*waydata.distant)/sum(waydata.distant)
            except:
                datanow[i]=float('nan')
        else:
            datanow[i]=float('nan')
    todb=pd.DataFrame([datanow])
    todb['day_of_week']=todb['time'].dt.dayofweek
    todb['hour']=todb['time'].dt.hour
    todb['minute']=todb['time'].dt.minute
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db='itictraveltime', user=uname, pw=pwd))
    todb.to_sql('traveltimelink', engine, index=False,if_exists="append")
    #print("--- %s seconds ---" % (time.time() - start_time))
    return

process10min(datetime.now(thai))

