#!/usr/bin/env python3
import pandas as pd
import numpy as np
import h5py
from radar.util.common import PD_HDF_TYPES

STATIC_LEN_STR_FIELDS = (
    'key.projectId',
    'key.userId',
    'key.sourceId',
)

def hdf_compat_dtypes(data):
    if 'datetime64' in str(data.dtype):
        d = data.astype(np.int64)
    elif 'object' in str(data.dtype):
        # if data.name in STATIC_LEN_STR_FIELDS:
        d = data.astype(np.string_)
    else:
        return data
    return d

def append_hdf5(dataframe, hdf5, user_id, schema=None, source=None):
    if not (schema or source):
        raise ValueError('schema or source key-word argument must be supplied')
    if not source:
        source = shema.name

    if type(dataframe.index) != pd.core.indexes.range.RangeIndex:
        dataframe = dataframe.reset_index(level=0)

    h5_data_path = user_id + '/' + source
    if not h5_data_path in hdf5:
        hdf5.create_group(h5_data_path)

    for col in dataframe.keys():
        if col in hdf5[h5_data_path]:
            append_dataset(hdf5, h5_data_path, col, dataframe[col])
        else:
            make_dataset(hdf5, h5_data_path, col, dataframe[col])


def read_hdf5(hdf5, user_id, source_ids='all'):
    if source_ids == 'all':
        source_ids = list(hdf5['user_id'].keys())
    df =  -1
    return df

def make_dataset(hdf5, key, name, data, dtype=None):
    if data.dtype == np.dtype('O') and dtype==None:
        dtype = h5py.special_dtype(vlen=str)
    hdf5[key].create_dataset(name, data=hdf_compat_dtypes(data),
                             maxshape=(None,), dtype=dtype)
    hdf5[key+'/'+name].attrs.create('dtype', str(data.dtype), dtype='S10')

def append_dataset(hdf5, key, name, data):
    datalen = len(data)
    hdf5[key+'/'+name].resize(hdf5[key+'/'+name].shape[0]+datalen, axis=0)
    hdf5[key+'/'+name][-datalen:] = hdf_compat_dtypes(data)


