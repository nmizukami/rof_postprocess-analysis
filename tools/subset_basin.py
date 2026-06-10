#!/usr/bin/env python
# -*- coding: utf-8 -*-

# From Hongli

import argparse
import numpy as np
import geopandas as gpd
import os
from sys import exit

id_name = 'seg_id'
downid_name = 'Tosegment'

def process_command_line():
    '''Parse the commandline'''
    parser = argparse.ArgumentParser(description=
             'Script to idenfity catchment or reach ids based on outlet id. require downstream element id')
    parser.add_argument('shpfile',
                        help='path of the catchmet/reach shapefile.')
    parser.add_argument('outlet_id',
                        help='Id of the outlet')
    parser.add_argument('outshp',
                        help='output shp')
    args = parser.parse_args()
    return(args)


def unique(list1):
    list_uniqe = []
    for x in list1:
        if not x in list_uniqe:
            list_uniqe.append(x)
    return list_uniqe

# main
if __name__ == '__main__':

    # process command line
    args = process_command_line()

    # read shapefile
    print('read shapefile')
    data = gpd.read_file(args.shpfile)

    if not id_name in data.columns.values:
        exit('%s column does not exist in shapefile.'%id_name)
    else:
        elem = data[id_name].values

    if not downid_name in data.columns.values:
        exit('%s column does not exist in shapefile.'%downid_name)
    else:
        nextElem = data[downid_name].values

    # search upstream elements
    id_int = int(args.outlet_id)
    all_ups_ids = [id_int]
    immediate_ups_ids = np.unique(elem[np.where(nextElem==id_int)])
    all_ups_ids.extend(list(immediate_ups_ids))
    round_num = 0
    print(id_int, immediate_ups_ids)
    while len(immediate_ups_ids) != 0:

        round_num = round_num+1
        print("Round %d. Totally %d elements are found." % (round_num, len(all_ups_ids)))

        # search upstream elem
        immediate_ups_ids_next = []
        for huc_i in immediate_ups_ids:
            immediate_ups_ids_next.extend(list(elem[np.where(nextElem==huc_i)]))
        immediate_ups_ids_next = np.unique(immediate_ups_ids_next)

        # identify if found HUC exists in upstrm_elem
        immediate_ups_ids = [huc for huc in immediate_ups_ids_next if not huc in all_ups_ids]
        all_ups_ids.extend(immediate_ups_ids)

    # save
    #outshape = os.path.basename(args.shpfile).replace('.gpkg','_%s.gpkg'%args.outlet_id)
    data[data[id_name].isin(all_ups_ids)].to_file(args.outshp, layer='HDMA', driver="GPKG")

    outasc = args.outshp.replace('.gpkg','.asc')
    np.savetxt(outasc, all_ups_ids, fmt='%s')
