# 2022 Dec 25
# Prabuddha Madusanka

#========================================================================================
# conda environment = pmtest

import cdsapi
import os
from os.path import exists
import datetime
from datetime import datetime
import s3fs
import openaq
import pandas as pd


fs = s3fs.S3FileSystem(anon=True)
c = cdsapi.Client()
api = openaq.OpenAQ()



year = ['2022']
month = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10']
#day = ['02']
day = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
#hour = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00', '08:00', '09:00', '10:00', '11:00', '12:00', '13:00', '14:00', '15:00', '16:00', '17:00', '18:00', '19:00',
#        '20:00', '21:00', '22:00', '23:00']
#hour = ['03']
hour = ['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']

#time = hour[0] + ':00'


area = [50, -125, 25, -65]  # US
grid = [0.1, 0.1]

download_directory = "/scratch/prabuddha/2yrHist_train/rawData/meteo_AOD/"




def down_meteo(year, month, day, time, meteo_path):
    try:
        c.retrieve(
            'reanalysis-era5-single-levels',
            {   
                'product_type': 'reanalysis',
                'format': 'grib',
                #'variable': '2m_temperature',
                'variable': ['2m_temperature', '2m_dewpoint_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind', 
                'surface_pressure', 'total_precipitation', 'skin_reservoir_content', 'evaporation', 'boundary_layer_height',
                'lake_cover', 'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation','precipitation_type', 'snowfall',
                'surface_net_solar_radiation_clear_sky','total_cloud_cover'],
                'year': year,
                'month': month,
                'day': day,
                'time': time,
                'area' : area,
                'grid' : grid,
            }, meteo_path)
    except:
        print('no meteo data on ' + year + '_' + month + '_' + day + '_' + time)

def down_humid(year, month, day, time, humid_path):
    try:
        data = c.retrieve('reanalysis-era5-pressure-levels',
            {
            'product_type': 'reanalysis',
            'variable': ['relative_humidity', 'specific_humidity', 'specific_rain_water_content'], 
            'pressure_level': '1000',
            'year': year,
            'month': month,
            'day': day,
            'time': time,
            #'format': 'grib',			# Supported format: grib and netcdf. Default: grib
            'area' : area, 
            'grid' : grid,		# Latitude/longitude grid.  (one latitude devided into 10 parts)         Default: 0.25 x 0.25  
            }, humid_path)
    except:
        print('no humid data on ' + year + '_' + month + '_' + day + '_' + time)

def down_AOD(year, month, day, hour, AOD_path):
    datetime_str = year + '-' + month + '-' + day + ' ' + hour + ':00:00'
    try:
        date_time = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        julian_day = date_time.strftime('%j')
        digit = len(str(julian_day))

        if digit == 1:
    	        julian_day = '0' + '0' + str(julian_day)
        elif digit == 2:
  	        julian_day = '0' + str(julian_day)
        else:
  	        julian_day = str(julian_day)

        AWS_path = 'noaa-goes16/ABI-L2-AODC/' + str(year) + '/' + julian_day + '/' + hour + '/*'

        files = fs.glob(AWS_path)
        fs.get(files[0], AOD_path)
    except:
        print('no AOD data on' + datetime_str)


def down_openAq(year, month, day, hour, OpenAQ_path):
    strt_date = year + '-' + month + '-' + day + 'T' + hour + ':00:00'
    end_date = year + '-' + month + '-' + day + 'T' + hour + ':10:00'
    try:
        resp = api.measurements(country = 'US', parameter = 'pm25', date_from = strt_date, date_to = end_date, df=True, limit = 10000)
        #resp = api.measurements(country = 'US', parameter = 'pm25', date_from = '2022-10-01T19:50:00', date_to = '2022-10-01T20:10:00', df=True, limit = 10000)
        resp.to_csv(OpenAQ_path)
    except:
        print('no OpenAQ data on ' + strt_date)









for y in year:
    try:
        os.mkdir(download_directory + str(y))
    except FileExistsError:
        print("already exists")
    
    for m in month: 
        try:
            os.mkdir(download_directory + str(y) + '/' + str(m))
        except FileExistsError:
            print("month folder exists")

        for d in day:
            try:
                os.mkdir(download_directory + str(y) + '/' + str(m) + '/' + str(d))
            except FileExistsError:
                print("day folder exists")

            for h in hour:
                try:
                    os.mkdir(download_directory + str(y) + '/' + str(m) + '/' + str(d) + '/' + str(h))
                except FileExistsError:
                    print("hour folder exists")

                meteo_path = download_directory + str(y) + '/' + str(m) + '/' + str(d) + '/' + str(h) + '/' + 'meteoro'
                humid_path = download_directory + str(y) + '/' + str(m) + '/' + str(d) + '/' + str(h) + '/' + 'humidity'
                AOD_path = download_directory + str(y) + '/' + str(m) + '/' + str(d) + '/' + str(h) + '/' + 'AOD'
                t = h + ':00'
                OpenAQ_path = download_directory + str(y) + '/' + str(m) + '/' + str(d) + '/' + str(h) + '/' + 'OpenAQ_pm.csv'

                if exists(meteo_path):
                    print("meteorogical data exist")
                else:
                    down_meteo(y, m, d, t, meteo_path)

                if exists(humid_path):
                    print("humidity data exist")
                else:
                    down_humid(y, m, d, t, humid_path)

                if exists(AOD_path):
                    print("AOD data exist")
                else:
                    down_AOD(y, m, d, h, AOD_path)

                if exists(OpenAQ_path):
                    print("OpenAQ data exist")
                else:
                    down_openAq(y, m, d, h, OpenAQ_path)

"""
try: 
    os.mkdir(download_directory + str(year[0]))
except FileExistsError:
    print("already exists")

try:
    os.mkdir(download_directory + str(year[0]) + '/' + str(month[0]))
except FileExistsError:
    print("month folder exists")

try:
    os.mkdir(download_directory + str(year[0]) + '/' + str(month[0]) + '/' + str(day[0]))
except FileExistsError:
    print("day folder exists")

try:
    os.mkdir(download_directory + str(year[0]) + '/' + str(month[0]) + '/' + str(day[0]) + '/' + str(hour[0]))
except FileExistsError:
    print("hour folder exists")


meteo_path = download_directory + str(year[0]) + '/' + str(month[0]) + '/' + str(day[0]) + '/' + str(hour[0]) + '/' + 'meteoro'


def down_meteo():
    c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'grib',
            #'variable': '2m_temperature',
            'variable': ['2m_temperature', '2m_dewpoint_temperature', '10m_u_component_of_wind', '10m_v_component_of_wind', 
            'surface_pressure', 'total_precipitation', 'skin_reservoir_content', 'evaporation', 'boundary_layer_height'],
            'year': year[0],
            'month': month[0],
            'day': day[0],
            'time': time,
            'area' : area,
            'grid' : grid,
        }, meteo_path)






if exists(meteo_path):
    print("meteorogical data exist")
else:
    down_meteo()
"""



print("done")
