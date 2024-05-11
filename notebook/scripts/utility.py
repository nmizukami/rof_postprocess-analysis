import tomli
import geopandas as gpd
import fiona
import pandas as pd
import numpy as np

def load_toml(toml_file) -> dict:
    """Load TOML data from file """
    with open(toml_file, 'rb') as f:
        return tomli.load(f)

class AutoVivification(dict):
    """Implementation of perl's autovivification feature."""
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


def records(filename, usecols, **kwargs):
    """Load geopackage or shapefile with selected attributes as objects """
    with fiona.open(filename, 'r') as src:
        for feature in src:
            f = {k: feature[k] for k in ['id', 'geometry']}
            f['properties'] = {k: feature['properties'][k] for k in usecols}
            yield f

            
def read_shps(shp_list,  usecols, **kwargs):
    """Load shapefiles with selected attributes in dataframe"""
    gdf_frame = []
    for shp in shp_list:
        gdf_frame.append(gpd.GeoDataFrame.from_features(records(shp, usecols, **kwargs)))
        print('Finished reading %s'%shp.strip('\n'))
    df_catch = pd.concat(gdf_frame)
    return df_catch


def get_index_array(a_array, b_array):
    ''' 
    Get index array where each index points to locataion in a_array. The order of index array corresponds to b_array

      e.g.,
      a_array = [2, 4, 1, 8, 3, 10, 5, 9, 7, 6]
      b_array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      result  = [2, 0, 4, 1, 6, 9, 8, 3, 7, 5]

    https://stackoverflow.com/questions/8251541/numpy-for-every-element-in-one-array-find-the-index-in-another-array
    '''
    index = np.argsort(a_array)
    sorted_a_array = a_array[index]
    sorted_index = np.searchsorted(sorted_a_array, b_array)

    yindex = np.take(index, sorted_index, mode="clip")
    mask = a_array[yindex] != b_array

    result = np.ma.array(yindex, mask=mask)

    return result


def reorder_index(ID_array_orig, ID_array_target):
    x = ID_array_orig
    # Find the indices of the reordered array
    # From: https://stackoverflow.com/questions/8251541/numpy-for-every-element-in-one-array-find-the-index-in-another-array
    index = np.argsort(x)
    sorted_x = x[index]
    sorted_index = np.searchsorted(sorted_x, ID_array_target)

    return np.take(index, sorted_index, mode="clip")


def no_time_variable(ds):
    vars_without_time = []
    for var in ds.variables:
        if 'time' not in ds[var].dims:
            if var not in list(ds.coords):
                vars_without_time.append(var)
    return vars_without_time