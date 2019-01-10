import struct
import os
import sys
import datetime
import sqlite3
import time
from config import logger

path = "./VWP"
Height_Text = [300,600,900,1200,1500,1800,2100,2400,2700,3000,
					  3400,3700,4000,4300,4600,4900,5200,5500,5800,6100,	
					 6700, 7300,7600,7900,8500,9100,10700,12200,13700,15200]
Height_WID = [500,1000,1500,2000,2500,3000,3500,4000,4500,5000,
					5500,6000,6500,7000,7500,8000,8500,9000,9500,10000]

tagVMP_RadarProductHeaderStructure = '!'+ 'h'+'h'+'i'+'i'+'h'+'h'+'h'

tagVMP_DescriptionStructure = '!h'+'i'+'i'+'h'+'h'+'h'*5+'i'+'h'+'i'+'h'*4+'h'*16+'h'*8+'i'*3

tagVWP_Symbol = '!'+'h'*2+'i'+'h'

tagVMP_PUB_LAYER = '!h'+'i'

tagVMP_PUB_BLOCK = '!h'+'h'

tagPUPRadarDataPackage0004 = '!'+'h'*5

tagPUPRadarDataPackage0A = '!' +'h'*4
# with open('Z_RADR_I_Z9543_20180914075900_P_DOR_SA_VWP_20_NUL_NUL.543.bin','rb+') as file:

def ReadVWP(filename,station_code,filedate):
    '''
    Read file write to _WID_list

    Args:
        filename:string of file name
        station_code:string of station
        filedata:

    Returns:
        a list of wind info(a dictionary)
    '''
    file = open(filename,'rb+')
    PUPRadarHeader = file.read(18)
    try:
        productCode,date,time,length,stationID,destinationID,blockCount = struct.unpack(tagVMP_RadarProductHeaderStructure,PUPRadarHeader)
    except ValueError:
        file.close()
        logger.error('%s:PUPRadarHeader ValueError'%filename)
    PUPRadarDescription = file.read(102)
    levelThreshold=[0 for x in range(0, 16)]  
    try:
        blockDivider,latitude,longitude,height,productCode,mode,volumeCoveragePattern,sequenceNumber,volumeScanNumber,volumeScanDate,volumeScanTime,productGenerationDate,productGenerationTime,p1,p2,elevationNumber,p3,levelThreshold[0],levelThreshold[1],levelThreshold[2],levelThreshold[3],levelThreshold[4],levelThreshold[5],levelThreshold[6],levelThreshold[7],levelThreshold[8],levelThreshold[9],levelThreshold[10],levelThreshold[11],levelThreshold[12],levelThreshold[13],levelThreshold[14],levelThreshold[15],p4,p5,p6,p7,p8,p9,p10,numberOfMaps,offsetToSymbol,offsetToGraphic,offsetToText=struct.unpack(tagVMP_DescriptionStructure,PUPRadarDescription)
        # print latitude,longitude
    except:
        file.close()
        logger.error('%s:PUPRadarHeader ValueError'%filename)
        return
    if productCode == 48:
        PUPRadarSymbol= file.read(10)
        try:
            blockDivider,blockID,blockSize,layerCount = struct.unpack(tagVWP_Symbol,PUPRadarSymbol)
        except:
            file.close()
            logger.error('%s:PUPRadarSymbol ValueError'%filename)
            return
    for i in range(layerCount):
        try:
            RADAR_Layer = file.read(6)
            layerDivider,layerCount = struct.unpack(tagVMP_PUB_LAYER,RADAR_Layer)
        except:
            file.close()
            logger.error('%s RADAR_Layer %d ValueError'%(filename,i))
            return
        if layerCount == 1:
            layerCount = blockSize - 16
        v_data_Pack_04 = []
        v_data_Pack_0A = []
        if layerCount > 0:
            sPosition = file.tell()
            # print 'sPosition'+str(sPosition)
            position = 0
            while position < layerCount:
                try:
                    RADAR_Block = file.read(4)
                    packageCode,newblocksize= struct.unpack(tagVMP_PUB_BLOCK,RADAR_Block)
                except:
                    file.close()
                    logger.error('RADAR_Block ValueError')
                    return
    #             print packageCode,newblocksize
    #             a = struct.unpack('s',hex(packageCode))
                packageCode = str(hex(packageCode))[2:]
    #             print str(packageCode).upper()
                if str(packageCode).upper() == '4':
                    RadarDataPackage04 = file.read(10)
                    level,x,y,direction,speed = struct.unpack(tagPUPRadarDataPackage0004,RadarDataPackage04)
                    v_data_Pack_04.append([level,x,y,direction,speed])
                elif str(packageCode).upper() == 'A':
    #                 print 'packageCode = A'
                    level = file.read(2)
                    level = struct.unpack('h',level)
                    posA = 0
                    count = (newblocksize-2)/8
                    while posA < count:
                        RadarDataPackage0A = file.read(struct.calcsize(tagPUPRadarDataPackage0A))
                        startX,startY,endX,endY= struct.unpack(tagPUPRadarDataPackage0A,RadarDataPackage0A)
                        v_data_Pack_0A.append([startX,startY,endX,endY])
                        posA+=1
                elif str(packageCode).upper() == '8':
                    file.seek(newblocksize,1)
                else:
                    if newblocksize > blockSize:
                        file.close()
                        break
                    if newblocksize<=0 or newblocksize ==2048:
                        file.close()
                        break
    #                 print newblocksize
                    try:
    #                     print file.tell() 
                        file.seek(newblocksize,1)
                        
                    except:
                        pass
                position = file.tell() - sPosition
                # print 'position'+str(position)
                
    # print v_data_Pack_0A
    nheight_pos = [0 for i in range(30)]
    time_pos = [0 for i in range(11)]
    for i in range(30):
        nheight_pos[i] = v_data_Pack_0A[i+6][3]
        if nheight_pos[i]<0:
            file.close()
    sorted(nheight_pos)

    n_data_Pack_Size = len(v_data_Pack_0A) 
    if n_data_Pack_Size!=47:
        file.close()
        
    for j in range(11):
        time_pos[j] = v_data_Pack_0A[j+36][2]
        
    sorted(time_pos)
    # print time_pos

    v_data_wind=[]
    # print len(v_data_Pack_04)
    for j in range(len(v_data_Pack_04)):
        if v_data_Pack_04[j][1] == time_pos[0]:
            v_data_wind.append(v_data_Pack_04[j])
    # print nheight_pos
    # print time_pos
    # print v_data_wind
    v_Radar_VMP = []
    # vwp_Radar_info ={}

    for h in range(30):
        for ii in range(len(v_data_wind)):
            if v_data_wind[ii][2] == nheight_pos[h]:
                vwp_Radar_info ={}
                vwp_Radar_info['level'] = v_data_wind[ii][0]
                vwp_Radar_info['winddirection'] = v_data_wind[ii][3]
                vwp_Radar_info['windspeed'] = v_data_wind[ii][4]*1852/3600.0
                vwp_Radar_info['height'] = Height_Text[h]
                vwp_Radar_info['obs_time'] = filedate
                vwp_Radar_info['station_code'] = station_code
                v_Radar_VMP.append(vwp_Radar_info)
    v_WID_INFO = []
    # for i in v_Radar_VMP:
    #     print i
    v_WID_INFO = Cal_WID(v_Radar_VMP)
    
    if v_WID_INFO==False:
        return
    # print vwp_Radar_info
    # for i in v_WID_INFO:
    #     print i
    _WID_list = []
    for ia in v_WID_INFO:
        _WID = {}
        _WID['level'] = ia['level']
        _WID['winddirection'] = ia['winddirection']
        _WID['windspeed'] = ia['windspeed']
        _WID['height'] = ia['height1']
        _WID['obs_time'] = ia['obs_time']
        _WID['station_code'] = ia['station_code']
        _WID['latitude'] = latitude
        _WID['longitude'] = longitude
        _WID_list.append(_WID)
    return _WID_list

def Cal_WID(v_Radar_VMP):
    '''
    find the index which lowest diff of height2 and height1 
    '''
    v_WID_INFO = []
    nsize = len(v_Radar_VMP)

    # Radar_WID = {}
    if nsize > 0:
        for i in range(nsize):
            for nh in range(20):
                min1 = abs(Height_WID[nh] - v_Radar_VMP[i]['height'])
                if min1 < 500:
                    WID_INFO = {}
                    WID_INFO['height1'] = Height_WID[nh]
                    WID_INFO['height2'] = v_Radar_VMP[i]['height']
                    WID_INFO['station_code'] = v_Radar_VMP[i]['station_code']
                    WID_INFO['level'] = v_Radar_VMP[i]['level']
                    WID_INFO['winddirection'] = v_Radar_VMP[i]['winddirection']
                    WID_INFO['windspeed'] = v_Radar_VMP[i]['windspeed']
                    WID_INFO['obs_time'] = v_Radar_VMP[i]['obs_time']
                    v_WID_INFO.append(WID_INFO)
        # print len(v_WID_INFO)
        # for i in v_WID_INFO:
        #     print i
        if len(v_WID_INFO) > 0:
            sorted(v_WID_INFO, key=lambda WID_INFO:WID_INFO['height1'])
        #     i = 0
        #     while i < (len(v_WID_INFO)-1):
        #         if v_WID_INFO[i]['height1'] ==  v_WID_INFO[i+1]['height1']:
        #             if (abs(v_WID_INFO[i]['height2']- v_WID_INFO[i]['height1'])) > (abs(v_WID_INFO[i+1]['height2']- v_WID_INFO[i+1]['height1'])):
        #                 v_WID_INFO.pop(i)
        #             elif (abs(v_WID_INFO[i]['height2']- v_WID_INFO[i]['height1'])) <= (abs(v_WID_INFO[i+1]['height2']- v_WID_INFO[i+1]['height1'])):
        #                 v_WID_INFO.pop(i+1)
        #                 i+=1
        #             else:
        #                 i+=1
        #         i +=i
        dic = {}
        # print v_WID_INFO
        for i in range(len(v_WID_INFO)):
            differ = abs(v_WID_INFO[i]['height2'] - v_WID_INFO[i]['height1'])
            if v_WID_INFO[i]['height2'] in dic.keys():
                select = dic[v_WID_INFO[i]['height2']]
                if differ < abs(v_WID_INFO[select]['height2']-v_WID_INFO[select]['height1']):
                    dic[v_WID_INFO[i]['height2']] = i
            else:
                dic[v_WID_INFO[i]['height2']] = i
        # print dic
    
        v_WID_INFO = [i for i in v_WID_INFO if v_WID_INFO.index(i) in dic.values()]
        # for i in v_WID_INFO:
        #     print i
        new_dic = {}
        for i,WID in enumerate(v_WID_INFO):
            diff = abs(WID['height1']-WID['height2'])
            if WID['height1'] in new_dic.keys():
                j = new_dic[WID['height1']]
                if diff < abs(v_WID_INFO[j]['height1']-v_WID_INFO[j]['height2']):
                    new_dic[WID['height1']] = i
            else:
                new_dic[WID['height1']] = i
        new_v_WID_INFO = [i for i in v_WID_INFO if v_WID_INFO.index(i) in new_dic.values()]
            # for i in range(len(v_WID_INFO)):
            #     if v_WID_INFO[i]['height1'] ==  v_WID_INFO[i+1]['height1']:
                    # if (abs(v_WID_INFO[i]['height2']- v_WID_INFO[i]['height1'])) > (abs(v_WID_INFO[i+1]['height2']- v_WID_INFO[i+1]['height1'])):
                    #     del v_WID_INFO[i]
                    #     i-=1
                    # if (abs(v_WID_INFO[i]['height2']- v_WID_INFO[i]['height1'])) <= (abs(v_WID_INFO[i+1]['height2']- v_WID_INFO[i+1]['height1'])):
                    #     del v_WID_INFO[i+1]
                    #     i-=1
                    

    else:
        return False
    return new_v_WID_INFO

def FindVWP(path):
    '''Fetches files from path

    Args:
        path: string of path

    Returns:
        a list of path contains string"VWP" 

    '''
    list_name = []
    try:
        lists = os.listdir(path)
    except:
        print 'error filepath'
        sys.exit(1)
    if not lists:
        return
    for file in lists:
        file_path = os.path.join(path, file)  
        if os.path.isdir(file_path):  
            FindVWP(file_path)  
        else:  
            list_name.append(file_path)

    # first filter:reduce traversal 

    now = datetime.datetime.now() - datetime.timedelta(hours=8) 
    delta=datetime.timedelta(hours=1) 
    now.strftime('%Y%m%d%H')
    str_time1 = now.strftime('%Y%m%d%H')
    str_time2 = (now-delta).strftime('%Y%m%d%H')

    first = [i for i in list_name if i.find('VWP')!= -1 and i.find(str_time1)!= -1 or i.find(str_time2)!= -1]
    remove_file = [i for i in list_name if i not in first ]
    for i in remove_file:
        try:
            os.remove(i)
        except:
            pass


    final = []
    for file in first:
        time = file.split('_')[4]
        time_d = datetime.datetime(int(time[0:4]),int(time[4:6]),int(time[6:8]),int(time[8:10]),int(time[10:12]))
        delta = now - time_d
        if delta.days == 0 and delta.seconds <= 6*60:
            final.append(file)
    # print final

    return first
    
def DealVWP(filepath):
    # filefoder = FindVWP(fileVWP)
    # if len(filefoder) == 0:
    #     return
    filefoders = filepath
    filewithno = filefoders.split('.')
    file_info = filewithno[1].split('_')
    station_code = len(file_info[3])
    station_time = len(file_info[4])
    if station_code==5 and station_time==14:
        iminuteMax = int(file_info[4][10:14])
        iflagMax = -1
        if iminuteMax <=200:
            iflagMax = 0
        elif iminuteMax >200 and iminuteMax <= 800:
            iflagMax = 6
        elif iminuteMax >800 and iminuteMax <= 1400:
            iflagMax = 12
        elif iminuteMax >1400 and iminuteMax <= 2000:
            iflagMax = 18
        elif iminuteMax >2000 and iminuteMax <= 2600:
            iflagMax = 24
        elif iminuteMax >2600 and iminuteMax <= 3200:
            iflagMax = 30
        elif iminuteMax >3200 and iminuteMax <= 3800:
            iflagMax = 36
        elif iminuteMax >3800 and iminuteMax <= 4400:
            iflagMax = 42
        elif iminuteMax >4400 and iminuteMax <= 5000:
            iflagMax = 48
        elif iminuteMax >5000 and iminuteMax <= 5600:
            iflagMax = 54
        elif iminuteMax >5600:
            iflagMax = 60 
        switch ={0:'0000',6:'0600',12:'1200',18:'1800',24:'2400',30:'3000',
                36:'3600',42:'4200',48:'4800',54:'5400',60:'6000'}
        strMinu = switch.get(iflagMax)
        file_time = ''
        # dtrel = {}
        if iflagMax!=60:
            file_time = file_info[4][0:10] + strMinu
        else:
            file_time = file_info[4][0:10] + strMinu
            year = int(file_time[0:4])
            month = int(file_time[4:6])
            day = int(file_time[6:8])
            hour = int(file_time[8:10])
            datetimes = datetime.datetime(year,month,day,hour)+datetime.timedelta(hours=1)
            file_time = '%04d%02d%02d%02d'%(datetimes.year,datetimes.month,datetimes.day,datetimes.hour)
            # datetime.datetime()
            file_time = file_time + '0000'
        return ReadVWP(filefoders,file_info[3],file_time),file_time

def Dealtime(filepath):
    filefoders = filepath
    filewithno = filefoders.split('.')
    file_info = filewithno[1].split('_')
    station_code = len(file_info[3])
    station_time = len(file_info[4])
    if station_code==5 and station_time==14:
        iminuteMax = int(file_info[4][10:14])
        iflagMax = -1
        if iminuteMax <=200:
            iflagMax = 0
        elif iminuteMax >200 and iminuteMax <= 800:
            iflagMax = 6
        elif iminuteMax >800 and iminuteMax <= 1400:
            iflagMax = 12
        elif iminuteMax >1400 and iminuteMax <= 2000:
            iflagMax = 18
        elif iminuteMax >2000 and iminuteMax <= 2600:
            iflagMax = 24
        elif iminuteMax >2600 and iminuteMax <= 3200:
            iflagMax = 30
        elif iminuteMax >3200 and iminuteMax <= 3800:
            iflagMax = 36
        elif iminuteMax >3800 and iminuteMax <= 4400:
            iflagMax = 42
        elif iminuteMax >4400 and iminuteMax <= 5000:
            iflagMax = 48
        elif iminuteMax >5000 and iminuteMax <= 5600:
            iflagMax = 54
        elif iminuteMax >5600:
            iflagMax = 60 
        switch ={0:'0000',6:'0600',12:'1200',18:'1800',24:'2400',30:'3000',
                36:'3600',42:'4200',48:'4800',54:'5400',60:'6000'}
        strMinu = switch.get(iflagMax)
        file_time = ''
        the_time = datetime.datetime(int(file_info[4][0:4]),int(file_info[4][4:6]),int(file_info[4][6:8]),int(file_info[4][8:10])
                                     ,int(file_info[4][10:12]))
        if iflagMax!=60:
            file_time = file_info[4][0:10] + strMinu
            year = int(file_time[0:4])
            month = int(file_time[4:6])
            day = int(file_time[6:8])
            hour = int(file_time[8:10])
            minu =  int(file_time[10:12])
            datetimes = datetime.datetime(year,month,day,hour,minu)
            interval = (datetimes-the_time) if datetimes>the_time else (the_time-datetimes)
        else:
            file_time = file_info[4][0:10] + strMinu
            year = int(file_time[0:4])
            month = int(file_time[4:6])
            day = int(file_time[6:8])
            hour = int(file_time[8:10])
            datetimes = datetime.datetime(year,month,day,hour)+datetime.timedelta(hours=1)
            interval = (datetimes-the_time) if datetimes>the_time else (the_time-datetimes)
    return file_time,interval.days*3600*24 +interval.seconds

def filter_vwp(filefolder):
    sdic = {}
    for filename in filefolder:
        sta = filename.split('.')[1].split('_')[3]
        if sta not in sdic.keys():
            sdic[sta] = [filename]
        else:
            sdic[sta].append(filename)
    # print sdic
    files = []
    for stafiles in sdic.values():
        dic = {}
        # print 'stafiles',len(stafiles)
        for i,filename in enumerate(stafiles):
            time,interval = Dealtime(filename)
            if time not in dic.keys():
                dic[time] = [i,interval]
            else:
                if interval < dic[time][1]:
                    dic[time] = [i,interval]
        index = [dic[i][0] for i in dic.keys()]
        files += [stafiles[i] for i in index]
    return files

def Store_indatabase(path):
    conn = sqlite3.connect('combination.db')
    cursor  = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'"% 'vwp')
    if not cursor.fetchall():
        cursor.execute('create table vwp(time_1 datetime,time_2 varchar(10),level int,station_code varchar(10),height int,windspeed float,winddirection int,latitude float,longitude float,primary key(time_1,station_code,height))')
        conn.commit()
        logger.info('create table vwp')
    filefoder = FindVWP(path)
    files = filter_vwp(filefoder)
    print len(files)
    index = 0
    for file in files:
        # print index
        data,file_time = DealVWP(file)
        new_ft = '{year}-{month}-{day} {hour}:{minute}:{second}'.format(year=file_time[0:4],month=file_time[4:6],day=file_time[6:8],hour=file_time[8:10],minute=file_time[10:12],second=file_time[12:14])

        # index +=1
        if data is not None:
            for i in data:
                try:
                    cursor.execute("insert into vwp(time_1,time_2,level,station_code,height,windspeed,winddirection,latitude,longitude) values ('%s','%s','%d','%s','%d','%f','%f','%f','%f')"%(new_ft,file_time[0:10],i['level'],i['station_code'],i['height'],i['windspeed'],i['winddirection'],i['latitude'],i['longitude']))
                except:
                    pass
        conn.commit()
    logger.info('store in table vwp finish')
    cursor.close()
    conn.close()

def dealbin():
    while True:
        if time.localtime().tm_min % 6 ==5:
            Store_indatabase(path)
            time.sleep(60)
        else:
            time.sleep(60)

