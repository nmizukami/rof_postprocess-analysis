#!/usr/bin/env python
'''
This script extracts routed flow time series at segments specified in csv
from routed runoff netcdf and create subset of netcdf
'''

import os, sys
import time
import argparse
import tomli
import glob
from datetime import datetime
import xarray as xr
import pandas as pd
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


def read_text(file_name, col_name):
    ''' Read one column from text file '''
    try:
        df_gauge = pd.read_csv(file_name, usecols=[col_name], header=0)
    except (IOError):
        print("Cannot open %s"%File_name)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    return df_gauge[col_name].values.tolist()


def remove_variable(ds, dim_name_list):
    vars_removed = [var for var in ds.variables for dim in ds[var].dims if dim in dim_name_list]
    return vars_removed


def no_dim_variable(ds, dim_list):
    vars_all = [var for var in ds.variables]
    vars_all.remove('time')
    vars_with_dim = [var for var in ds.variables for dim in ds[var].dims if dim in dim_list]
    vars_without_dim = list(set(vars_all) - set(vars_with_dim))
    return vars_without_dim


def extract_nc(nc_in, id_varname, id_list, nc_out, ds_domain=None):
    '''
    input nc_in: input river network topology netcdf
          id_varname:  id variable name e.g., reachID
          id_list: list of selected seg_id or hru_id
          nc_out: output subset streamflow netcdf
    '''
    # read the netcdf file into dataset
    ds = xr.open_dataset(nc_in, chunks={'time':1})

    if ds_domain:
        ds[id_varname] = ds_domain[id_varname]
        # 2D -> 1D
        ds = ds.stack(seg=('lat', 'lon'))
        ds = ds.reset_index('seg').drop(['lat', 'lon'])

    # remove variables with non-seg dimension
    vars_without_seg = no_dim_variable(ds, ['seg'])

    if vars_without_seg:
        ds = ds.drop(vars_without_seg).load()
    else:
        ds = ds.load()

    # subset the netcdf file based on the hruId
    ds_subset = ds.where(ds[id_varname].isin(id_list), drop=True)

    # make sure that the subsetted types are the same as the original ones
    for var in ds_subset.variables:
        ds_subset[var] = ds_subset[var].astype(ds[var].dtype)

    # update the history attribute
    history = '{}: {}\n'.format(datetime.now().strftime('%c'),
                                ' '.join(sys.argv))
    if 'history' in ds_subset.attrs:
        ds_subset.attrs['history'] = history + ds_subset.attrs['history']
    else:
        ds_subset.attrs['history'] = history

    # Write to file
    ds_subset.load().to_netcdf(nc_out)

# -----------------------------------------
# Main
# -----------------------------------------
if __name__ == '__main__':

    args = process_command_line()

    config = load_toml(args.config)
    main_dir        = config['main_dir']
    case            = config['case']
    domain_nc       = config['domain_nc']
    reach_id_in_nc  = config['reachID']
    file_pattern    = config['file_pattern']
    ref_name        = config['ref_flow_name']
    site_reach_link = os.path.join(config['ref_flow_dir'],ref_name, config['site_reach_csv'])
    reach_id_in_csv = config['reach_id_csv']

    # initialize dask-mpi
    if useDask:
        dask_mpi.initialize()

    segids = read_text(site_reach_link, reach_id_in_csv)

    if os.path.isfile(domain_nc):
        ds_domain = xr.open_dataset(domain_nc)
    else:
        ds_domain=None

    case_dir = os.path.join(main_dir, case, 'rof/hist')

    with Client():
        nc_list = glob.glob(os.path.join(case_dir, f'{case}.{file_pattern}.nc'))
        for nc in nc_list:
            start_time = time.time()
            out_nc = nc.replace('.nc',f'.{ref_name}.nc')
            extract_nc(nc, reach_id_in_nc, segids, out_nc, ds_domain=ds_domain)
            print('Finished site extraction %s: %.4f [sec]'%(os.path.basename(nc), time.time()-start_time))
