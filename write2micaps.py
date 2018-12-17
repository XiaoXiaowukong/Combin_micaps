import sqlite3
import pandas as pd
import struct

def create_file(file,height,file_time):
    discriminator = struct.pack('4s','mdfs')
    type = struct.pack('<h',21)
    description = struct.pack('100s','')
    level = struct.pack('<f',height)
    levelDescription = struct.pack('50s','M')
    year = struct.pack('<i',int(file_time[0:4]))
    month = struct.pack('<i',int(file_time[4:6]))
    day = struct.pack('<i',int(file_time[6:8]))
    hour = struct.pack('<i',int(file_time[8:10]))
    minute = struct.pack('<i',int(file_time[10:12]))
    second = struct.pack('<i',int(file_time[12:14]))
    timezone = struct.pack('<i',8)
    file.write(discriminator)
    file.write(type)
    file.write(description)
    file.write(level)
    file.write(levelDescription)
    file.write(year)
    file.write(month)
    file.write(day)
    file.write(hour)
    file.write(minute)
    file.write(second)
    file.write(timezone)

    Extent = struct.pack('100s','0'*100)
    file.write(Extent)

def write_head_2(file,station_num):
    station_num = struct.pack('i',station_num)
    file.write(station_num)
    
    p_num = struct.pack('h',4)
    file.write(p_num)

    # # latitude
    # latitude_id = struct.pack('<h',2)
    # latitude_type = struct.pack('<h',5)
    # file.write(latitude_id)
    # file.write(latitude_type)
    # # longitude
    # longitude_id = struct.pack('<h',1)
    # longitude_type = struct.pack('<h',5)
    # file.write(longitude_id)
    # file.write(longitude_type)
    #str_station_name
    str_station_name_id = struct.pack('<h',21)
    str_station_type = struct.pack('<h',7)
    file.write(str_station_name_id)
    file.write(str_station_type)
    # # level
    # level_id = struct.pack('<h',3)
    # level_type = struct.pack('<h',2)
    # file.write(level_id)
    # file.write(level_type)
    # windspeed
    windspeed_id = struct.pack('<h',203)
    windspeed_type = struct.pack('<h',5)
    file.write(windspeed_id)
    file.write(windspeed_type)
    # windidrection
    winddirection_id = struct.pack('<h',201)
    winddirection_type = struct.pack('<h',5)
    file.write(winddirection_id)
    file.write(winddirection_type)

    height_id = struct.pack('<h',3)
    height_type = struct.pack('<h',5)
    file.write(height_id)
    file.write(height_type)

def write_head_3(file,station,lon,lat,num):
    # sta_id
    sta_id = struct.pack('<i',int(station))
    file.write(sta_id)
    # longitude
    longitude = struct.pack('<f',lon)
    file.write(longitude)
    # latitude
    latitude = struct.pack('<f',lat)
    file.write(latitude)
    # num
    num = struct.pack('<h',num)
    file.write(num)

def write_vwp_data(file,station,windspeed,winddirection,height):
    
    str_station_name_id = struct.pack('<h',21)
    str_station_name = struct.pack('10s',station)
    str_station_len = struct.pack('<h',10)
    file.write(str_station_name_id)
    file.write(str_station_len)
    file.write(str_station_name)
    
    # level_id = struct.pack('<h',3)
    # level = struct.pack('<h',level)
    # file.write(level_id)
    # file.write(level)
    
    windspeed_id = struct.pack('<h',203)
    windspeed = struct.pack('<f',windspeed)
    file.write(windspeed_id)
    file.write(windspeed)
    
    winddrection_id = struct.pack('<h',201)
    winddirection = struct.pack('<f',winddirection)
    file.write(winddrection_id)
    file.write(winddirection)

    height_id = struct.pack('<h',3)
    height = struct.pack('<f',height)
    file.write(height_id)
    file.write(height)
    
def write_OOBS_data(file,horizontalv,horizontald,height):
    # horizontalv
    horizontalv_id = struct.pack('<h',203)
    horizontalv = struct.pack('<f',horizontalv)
    file.write(horizontalv_id)
    file.write(horizontalv)
    
    # horizontald
    horizontald_id = struct.pack('<h',201)
    horizontald = struct.pack('<f',horizontald)
    file.write(horizontald_id)
    file.write(horizontald)

    #height
    height_id = struct.pack('<h',3)
    height = struct.pack('<f',height)
    file.write(height_id)
    file.write(height)

if __name__ == '__main__':
    with sqlite3.connect('combination.db') as con:
        OOBS = pd.read_sql_query("SELECT * FROM OOBS", con=con)
        vwp = pd.read_sql_query("SELECT * FROM vwp", con=con)

    for i,group in vwp.groupby(['time_1','height']):
        print 'time_1:',i[0],' height:',i[1]
        height = i[1]
        file = open(str(i[0])+'_'+str(i[1])+'.bin','ab')
        
        OOBS_bysta = OOBS[OOBS.time_2==i[0][0:10]][OOBS.height==i[1]].groupby('station_code')
        vwp_bysta = group.groupby('station_code')
        sta_num = len(list(set(list(OOBS[OOBS.time_2==i[0][0:10]][OOBS.height==i[1]]['station_code'])+list(group['station_code']))))
        stanum = 1
        print 'sta_num',sta_num
        create_file(file,i[1],i[0])
        write_head_2(file,sta_num)
        for station,OOBS_group in OOBS_bysta:
            lat = float(OOBS_group[OOBS_group.station_code==station].iloc[0]['latitude'])
            lon = float(OOBS_group[OOBS_group.station_code==station].iloc[0]['longitude'])
            OOBS_num = len(OOBS_group[OOBS_group.station_code==station])
            write_head_3(file,station,lat,lon,3)
            for row in OOBS_group.iterrows():
                write_OOBS_data(file,row[1]['horizontalv'],row[1]['horizontald'],height)
                print row[1]['horizontalv'],row[1]['horizontald']
                
        i = 0
        for station,vwp_group in vwp_bysta:
            lat = float(vwp_group[vwp_group.station_code==station].iloc[0]['latitude'])/1000
            lon = float(vwp_group[vwp_group.station_code==station].iloc[0]['longitude'])/1000
            print lat,lon
            vwp_num = len(vwp_group[vwp_group.station_code==station])
            print 'station',type(station)
            write_head_3(file,i,lon,lat,4)
            for row in vwp_group.iterrows():
                write_vwp_data(file,str(station),row[1]['windspeed'],row[1]['winddirection'],height)
                print row[1]['level'],row[1]['windspeed'],row[1]['winddirection']
            i+=1
        break