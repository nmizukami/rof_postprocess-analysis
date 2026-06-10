#!/usr/bin/env python

"""
Breif:
Compute monthly mean, annual mean, and mean annual cycle (daily or monthly step) from daily history file
This should work for either CESM ROF component -  mosart or mizuRoute
"""

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import os,sys
import argparse
import tomli
import time
import glob
from datetime import datetime, timedelta
import numpy as np
import xarray as xr
import dask_mpi
from dask.distributed import Client

useDask=False

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='Script to compute monthly, annual, and seasonal mean.')
    parser.add_argument('config', help='config file')
    args = parser.parse_args()
    return(args)

def load_toml(toml_file) -> dict:
    """Load TOML data from file """
    with open(toml_file, 'rb') as f:
        return tomli.load(f)

def no_time_variable(ds):
    vars_without_time = []
    for var in ds.variables:
        if 'time' not in ds[var].dims:
            if var not in list(ds.coords):
                vars_without_time.append(var)
    return vars_without_time

def time_aggregate(dir_in, agg_method='mean', day_shift=0):

    # daily to monthly
    nc_list = glob.glob(os.path.join(dir_in, f'{case}.{file_pattern}.nc'))
    print(os.path.join(dir_in, f'{case}.{file_pattern}.nc'))
    for nc in nc_list:
        start_time = time.time()
        ds_tmp = xr.open_dataset(nc, chunks={'time':1})
        if day_shift!=0:
            ds_tmp['time'] = ds_tmp.indexes['time'].shift(day_shift, "D")

        vars_no_time = no_time_variable(ds_tmp) # get variables without time dimension
        if agg_method=='mean':
           ds_tmp = ds_tmp.resample(time='1M').mean(keep_attrs=True)
        elif agg_method=='first':
           ds_tmp = ds_tmp.resample(time='1M').map(lambda x: x.isel(time=0))
        elif agg_method=='last':
           ds_tmp = ds_tmp.resample(time='1M').map(lambda x: x.isel(time=-1))
        ds_tmp[vars_no_time] = ds_tmp[vars_no_time].isel(time=0, drop=True) # drop time dimension for no time variables
        ds_mon = ds_tmp.load()

        out_nc = nc.replace('.nc','.month.nc')
        ds_mon.to_netcdf(out_nc)
        print('Finished computing monthly flow: %.4f [sec]'%(time.time()-start_time))

    # monthly to annual
    start_time = time.time()
    nc_list = sorted(glob.glob(os.path.join(dir_in, f'{case}.{file_pattern}.month.nc')))
    ds_tmp = xr.open_mfdataset(nc_list,data_vars='minimal') # read all the needed netcdfs (do not concatenate variable without time dimension
    vars_no_time = no_time_variable(ds_tmp) # get variables without time dimension
    if agg_method=='mean':
      ds_tmp = ds_tmp.resample(time='1A').mean(keep_attrs=True)
    elif agg_method=='first':
      ds_tmp = ds_tmp.resample(time='1A').map(lambda x: x.isel(time=0))
    elif agg_method=='last':
      ds_tmp = ds_tmp.resample(time='1A').map(lambda x: x.isel(time=-1))

    ds_tmp[vars_no_time] = ds_tmp[vars_no_time].isel(time=0, drop=True) # drop time dimension for no time variables
    ds_ann = ds_tmp.load()

    out_nc = nc_list[0].replace('.month.nc','.annual.nc') # use the first year for file name
    ds_ann.to_netcdf(out_nc)
    print('Finished computing annual flow: %.4f [sec]'%(time.time()-start_time))

def comp_seasonality(dir_in, time_period, time_resolution):

    start_time = time.time()
    if time_resolution=='month':
        nc_list = sorted(glob.glob(os.path.join(dir_in, f'{case}.{file_pattern}.month.nc')))
        ds_tmp = xr.open_mfdataset(nc_list, data_vars='minimal').sel(time=time_period)
        vars_no_time = no_time_variable(ds_tmp)  # get variables without time dimension
        ds_tmp = ds_tmp.groupby("time.month").mean(dim='time',keep_attrs=True).load()
        ds_tmp[vars_no_time] = ds_tmp[vars_no_time].isel(month=0, drop=True) # drop time dimension for no time variables
    elif time_resolution=='day':
        nc_list = sorted(glob.glob(os.path.join(dir_in, f'{case}.{file_pattern}.nc')))
        ds_tmp = xr.open_mfdataset(nc_list, data_vars='minimal').sel(time=time_period)
        vars_no_time = no_time_variable(ds_tmp)      # get variables without time dimension
        ds_tmp = ds_tmp.groupby("time.day").mean(dim='time',keep_attrs=True).load()
        ds_tmp[vars_no_time] = ds_tmp[vars_no_time].isel(day=0, drop=True) # drop time dimension for no time variables

    ds_season = ds_tmp.load()

    out_nc = nc_list[0].replace('.nc','.seasonal.nc') # use first year for file name
    ds_season.to_netcdf(out_nc)
    print('Finished computing seasonal flow: %.4f [sec]'%(time.time()-start_time))

if __name__ == '__main__':

    args = process_command_line()

    config = load_toml(args.config)
    main_dir     = config['main_dir']
    case         = config['case']
    file_pattern = config['file_pattern']
    day_shift    = config['day_shift']
    start_yr     = config['start_yr']
    end_yr       = config['end_yr']
    agg_method   = config['agg_method']
    time_res     = config['time_res']

    # initialize dask-mpi
    if useDask:
        dask_mpi.initialize()

    case_dir = os.path.join(main_dir, case, 'rof/hist')

    time_period_seasonality = slice(f'{start_yr}-01-01',f'{end_yr}-12-31')

    with Client():
        time_aggregate(case_dir, agg_method, day_shift=day_shift)
        #comp_seasonality(case_dir, time_period_seasonality, time_res)
