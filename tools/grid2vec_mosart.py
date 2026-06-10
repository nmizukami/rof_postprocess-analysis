#!/usr/bin/env python
'''
This script convert 2d gridded MOSART history file to 1d vector history file.
grid id is assigned to each grid box - starting at lower left (SW) corner of domain and incement in longitude first
Other process is mask out ocean grid boxes
'''

import os, sys
import time
import argparse
import tomli
import glob
from datetime import datetime
import xarray as xr
import dask_mpi
from dask.distributed import Client

useDask=False

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description='Script to subset a netcdf file based on a list of IDs.')
    parser.add_argument('config', help='config file')
    args = parser.parse_args()
    return(args)

def load_toml(toml_file) -> dict:
    """Load TOML data from file """
    with open(toml_file, 'rb') as f:
        return tomli.load(f)

def remove_variable(ds, dim_name_list):
    vars_removed = [var for var in ds.variables for dim in ds[var].dims if dim in dim_name_list]
    return vars_removed

def no_dim_variable(ds, dim_list):
    vars_all = [var for var in ds.variables]
    vars_all.remove('time')
    vars_with_dim = [var for var in ds.variables for dim in ds[var].dims if dim in dim_list]
    vars_without_dim = list(set(vars_all) - set(vars_with_dim))
    return vars_without_dim

def grid2vec(nc_in, mask_var, ocan_mask_n, domain_nc, gid_varname, nc_out):
    '''
    input nc_in: input river network topology netcdf
          id_varname:  id variable name e.g., reachID
          id_list: list of selected seg_id or hru_id
          nc_out: output subset streamflow netcdf
    '''

    # read the rof netcdf file into dataset
    ds_rof = xr.open_dataset(nc_in, chunks={'time':1})

    # read the domain netcdf file into dataset
    ds_domain = xr.open_dataset(domain_nc)
    ds_rof[gid_varname] = ds_domain[gid_varname]

    # 2D -> 1D
    ds_rof = ds_rof.stack(seg=('lat', 'lon'))
    ds_rof = ds_rof.reset_index('seg').load()

    vars_without_seg = no_dim_variable(ds_rof, ['seg'])

    # mask out ocean
    ds_rof = ds_rof.where(ds_rof[mask_var]!=ocean_mask_n, drop=True)

    if vars_without_seg:
        ds_rof = ds_rof.drop(vars_without_seg)
    else:
        ds_rof = ds_rof

    # make sure that the subsetted types are the same as the original ones
    for var in ds_rof.variables:
        ds_rof[var] = ds_rof[var].astype(ds_rof[var].dtype)

    # update the history attribute
    history = '{}: {}\n'.format(datetime.now().strftime('%c'),' '.join(sys.argv))
    if 'history' in ds_rof.attrs:
        ds_rof.attrs['history'] = history + ds_rof.attrs['history']
    else:
        ds_rof.attrs['history'] = history

    ds_rof.to_netcdf(nc_out)

# -----------------------------------------
# Main
# -----------------------------------------
if __name__ == '__main__':

    args = process_command_line()

    config = load_toml(args.config)
    main_dir        = config['main_dir']
    case            = config['case']
    file_pattern    = config['file_pattern']
    mask_var_in_nc  = config['mask_var']
    ocean_mask_n    = config['ocean_mask_n']
    domain_nc       = config['domain_nc']
    reach_id_in_nc  = config['reachID']

    # initialize dask-mpi
    if useDask:
        dask_mpi.initialize()

    case_dir = os.path.join(main_dir, case, 'rof/hist')
    nc_list = glob.glob(os.path.join(case_dir, f'{case}.{file_pattern}.nc'))

    with Client():

        for rof_nc in nc_list:
            start_time = time.time()
            out_nc = rof_nc.replace('.h0.',f'.vec.h0.')
            grid2vec(rof_nc, mask_var_in_nc, ocean_mask_n, domain_nc, reach_id_in_nc, out_nc)
            print('Finished vectorize %s: %.4f [sec]'%(os.path.basename(rof_nc), time.time()-start_time))
