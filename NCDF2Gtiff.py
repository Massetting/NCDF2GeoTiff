# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 16:36:17 2018

@author: Andrea Massetti

# =============================================================================
# Script to export spatial datasets into GeoTiff from netcdf
# =============================================================================
"""
__version__ = "1.0"
import os 
import gdal
import xarray as xr
from pandas import to_datetime as t_dt
from dask.distributed import Client
import functools
import numpy as np
import argparse
import datetime as dt

def save(driver, geot, proj, ds, naming, sx, sy, outfolder, dtype=gdal.GDT_Int16, nodata=-999):
    """
    ARGS:

        - driver : (object: osgeo.gdal.Driver)
        - geot : (tuple) gdal geotransform object
        - proj : (str) gdal projection
        - ds : (object: masked numpy 2D array)
        - sx : number of cols
        - sy : number of rows
        - outfolder : path for the output; default is same as input
        - dtype : Geotiff datatype. default is integer 16 bit. This will discard decimal data. 
            This is important for disk space: a floating point will be larger: better to implement a multiplication and convert to integer (ie if the data is in %, just multiply by 10 000 and keep as int 2 decimal digits) 
             	Other gdal datatypes (not tested)
                gdal.GDT_Byte
                gdal.GDT_UInt16
                gdal.GDT_Int16
                gdal.GDT_UInt32
                gdal.GDT_Int32
                gdal.GDT_Float32
                gdal.GDT_Float64
                gdal.GDT_CInt16
                gdal.GDT_CInt32
                gdal.GDT_CFloat32
                gdal.GDT_CFloat64
        - nodata : value assigned to nodata pixels.
    """
    fullpath = os.path.join(out_fld, naming)
    new_tiff = driver.Create(fullpath, sx, sy, 1, dtype)
    new_tiff.SetGeoTransform(geot)
    new_tiff.SetProjection(proj)
    ds[np.isnan(ds)] = -999
    new_tiff.GetRasterBand(1).WriteArray(ds)
    new_tiff.GetRasterBand(1).SetNoDataValue(nodata)
    new_tiff.FlushCache() 
    new_tiff = None

        
def unpack(name, destination, variables_names={'blue':'b1','green':'b2','red':'b3','nir':'b4','swir1':'b5','swir2':'b7'}, dates=False):
    """
# =============================================================================
     unpack opens a netcdf, reads the metatada and calls save method. 
     It will save as geotiff all the variables in variables_names dictionary
# =============================================================================

    ARGS:
        - name (str): the NetCdf file name (relative path)
        - variables_names (dict) This dictionary gives the name of the variable and an id for the output file. 
            Example: variables_names={'blue':'b1','green':'b2','red':'b3'}, will unpack the variables named blue, green and red and name them as b1, b2 and b3.
            default for geoscience australia Landsat TM and ETM: variables_names={'blue':'b1','green':'b2','red':'b3','nir':'b4','swir1':'b5','swir2':'b7'}
            NOTE:
                If I want to save only certain bands, I can modify the band_string variable.
                EX. If I want only visible bands: band_string={'blue':'b1','green':'b2','red':'b3'}
        - dates (list): list of two strings representing the start and end dates in format: "dd/mm/YYYY"
            Example: dates = ["02/02/2005","31/12/2005"]
    """
    identification = name.split(".")[0] #change as needed, it is the naming id from the ncdf. it will remain the same for all 
    net = os.path.join(path,name)
    ncv = xr.open_dataset(net)
    time = ncv.time.values
    date = [t_dt(f) for f in time]

    ds_ph = gdal.Open('NETCDF:"'+ net+ '":{}'.format([f for f in variables_names.keys()][0]))
#    ds_ph = gdal.Open('NETCDF:"'+ net+ '":__xarray_dataarray_variable__')
    sx = ds_ph.RasterXSize
    sy = ds_ph.RasterYSize
    geot = ds_ph.GetGeoTransform()
    proj = """PROJCS["GDA94 / Australian Albers",GEOGCS["GDA94",DATUM["Geocentric_Datum_of_Australia_1994",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4283"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",-18],PARAMETER["standard_parallel_2",-36],PARAMETER["latitude_of_center",0],PARAMETER["longitude_of_center",132],PARAMETER["false_easting",0],PARAMETER["false_northing",0],AUTHORITY["EPSG","3577"],AXIS["Easting",EAST],AXIS["Northing",NORTH]]"""#ds_ph.GetProjection()
    ds_ph = None
    driver = gdal.GetDriverByName('GTiff')
#    dates = [t_dt("02/02/2005", format='%d/%m/%Y'), t_dt("2/6/2005", format='%d/%m/%Y')]
    if dates:
        date_min = t_dt(dates[0], format='%d/%m/%Y')
        date_max = t_dt(dates[1], format='%d/%m/%Y')
    else:
        date_min = date[0]
        date_max = date[-1]
    for i in range(len(date)):
        if (date[i] > date_min) & (date[i] < date_max):
            print(date[i])
            for key, val in vn.items():
                try:
                    filename = "{}_{}_{}.tif".format(date[i].strftime("%Y-%m-%d"), identification, val)
                    ds = ncv[key].isel(time=i).values
                    save(driver, geot, proj, ds, filename, sx, sy, destination)
                    with open(os.path.join(path,"log.txt"),'a') as log:
                        log.write(f"{dt.datetime.now()}: {filename}      saved successfully\n")
                except:
                    with open(os.path.join(path,"log.txt"),'a') as log:
                        log.write(f"{dt.datetime.now()}: PROBLEM WITH {filename}\n")
#                    print("problem with {}__{}".format(name,date[i])) 

if __name__=="__main__":
    parser = argparse.ArgumentParser(prog="NCDF2Gtiff", description=
                                     'Converts a netcdf variable(s) in a date range into single geotiff files')
    #TODO! add the argument for the variables. I do not need it now as I only extract the same 1 variable
    parser.add_argument('-n', dest="site", default=None, type=str,
                        help='''Site name of the location. 
                        The default behaviour expects to have the netcdf files 
                        at os.path.join("/g/data3/xg9/vspi", site)''')
    parser.add_argument('--date-range', dest="dates", type=str, nargs=2 ,
                        help="""date range. Provide two dates in format dd/mm/yyyy
                            first the start date and then the end date 
                            example :
                                --date-range "01/01/2005" "31/12/2018"
                                """)
    parser.add_argument('--version', action='version', version=__version__)
    jj = parser.parse_args()
    site = jj.site
    dates = jj.dates
    path = os.path.join("/g/data3/xg9/vspi", site)
    names = [f for f in os.listdir(path) if f.endswith(".nc")]
    out_fld = os.path.join(path, "tiff")
    if not os.path.isdir(out_fld):
        os.makedirs(out_fld)
        with open(os.path.join(path,"log.txt"),'a') as log:
            log.write(f"{dt.datetime.now()}: Created container folder for tiff\n")
    vn = {"VSPI": "VSPI"}
    with open(os.path.join(path,"log.txt"),'a') as log:
        log.write(f"{dt.datetime.now()}: Starting tiff export at {out_fld}\n ORIGIN FILES:\n ###")
        for ncdf in names: 
            log.write(ncdf)
    client = Client()
#    dt = ["01/01/2005","31/12/2018"]
    j=functools.partial(unpack, destination=path, variables_names=vn, dates=dates)
    a=client.map(j, names)  
    for we in a:
        we.result()
    with open(os.path.join(path,"log.txt"),'a') as log:
        log.write(f"{dt.datetime.now()}: Completed tiff export")
