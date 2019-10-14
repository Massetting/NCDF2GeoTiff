# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 16:36:17 2018

@author: Andrea Massetti

# =============================================================================
# Script to export spatial datasets into GeoTiff from netcdf
array version:
    I just want a function that I can import somewhere else that lets me
    save as tiff a xarray variable
# =============================================================================
"""
__version__ = "1.1"
import os
import gdal
import xarray as xr
from pandas import to_datetime as t_dt
from dask.distributed import Client
import functools
import numpy as np
import argparse
import datetime as dt

def save(driver, geot, proj, ds, destination, sx, sy, dtype=gdal.GDT_Int16, nodata=-999):
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
    new_tiff = driver.Create(destination, sx, sy, 1, dtype)
    new_tiff.SetGeoTransform(geot)
    new_tiff.SetProjection(proj)
    ds[np.isnan(ds)] = -999
    new_tiff.GetRasterBand(1).WriteArray(ds)
    new_tiff.GetRasterBand(1).SetNoDataValue(nodata)
    new_tiff.FlushCache()
    new_tiff = None

#variable = f.NDVI.sel(time=f.time[0])
def save_var(variable, destination, projection=None, dtype=gdal.GDT_Int16):
    """
    variable must be an xarray DataArray (before the ´.values´)
    destination must be a full file path (string, not pathlib.Path)
    projection must be taken before the netcdf is handled, because
    the conversion between DataArray and DataSet loses attrs
    """

#    proj = k.attrs['projection']
    var = variable.values
    sy, sx = var.shape
    geot = (int(variable.x[0].values), 30, 0, int(variable.y[0].values), 0, -30)

    driver = gdal.GetDriverByName('GTiff')
#    dates = [t_dt("02/02/2005", format='%d/%m/%Y'), t_dt("2/6/2005", format='%d/%m/%Y')]
    save(driver, geot, projection, var, destination, sx, sy, dtype=dtype)
