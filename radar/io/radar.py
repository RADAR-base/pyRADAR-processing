#!/usr/bin/env python3
import glob
import pandas as pd
import dask.delayed as delayed
import dask.dataframe as dd
from functools import wraps
from .generic import _data_load_funcs
from ..common import config
from ..util.armt import melt, populate, infer_questionId

def delayed_read(func, *args, **kwargs):
    def read(path):
        df = func(path, *args, **kwargs)
        return df
    return read

def file_datehour(fn):
    return pd.Timestamp(fn.split('/')[-1][0:13].replace('_', 'T'))

def read_csv_func(func):
    @wraps(func)
    def wrapper(path, *args, **kwargs):
        files = glob.glob(path + '/*.csv*')
        files.sort()
        divisions = [file_datehour(fn) for fn in files] +\
                    [file_datehour(files[-1]) + pd.Timedelta(1, 'h')]
        delayed_files = [delayed(func(*args, **kwargs))(fn)
                                 for fn in files]
        return dd.from_delayed(delayed_files, divisions=divisions)
    return wrapper

@read_csv_func
def read_prmt_csv(dtype=None, timecols=None, index=config.io.index):
    if dtype is None:
        dtype = {}
    if timecols is None:
        timecols = []
    def read_csv(path, *args, **kwargs):
        df = pd.read_csv(path, *args, dtype=dtype, **kwargs)
        for col in timecols:
            df[col] = (1e9 * df[col].values).astype('datetime64[ns]')
        df = df.set_index(index)
        df = df.sort_index()
        return df
    return read_csv

@read_csv_func
def read_armt_csv(index=config.io.index):
    def read_csv(path, *args, **kwargs):
        df = pd.read_csv(path, *args, **kwargs)
        df = melt(df)
        for col in ('value.time', 'value.timeCompleted',
                    'startTime', 'endTime'):
            df[col] = (1e9 * df[col].values).astype('datetime64[ns]')
        df['arrid'] = df.index
        df = df.set_index(index)
        df = df.sort_index(kind='mergesort')
        return df
    return read_csv

def schema_read_csv_funcs(schemas, *args, **kwargs):
    """
    Adds read_csv functions for each schema name to the _data_load_funcs
    in radar.io.generic
    Parameters
    __________
    schemas: dict
        Dictionary containing keys of schema names with RadarSchema values
    """
    for name, scm in schemas.items():
        _data_load_funcs[name] =\
                delayed_read(read_prmt_csv, scm.dtype(), scm.timecols())

def armt_read_csv_funcs(protocol):
    for armt in protocol.values():
        name = armt.questionnaire.avsc + '_' + armt.questionnaire.name
        _data_load_funcs[name] = delayed_read(read_armt_csv)
    pass

if config.schema.read_csvs:
    from ..util import schemas as _schemas
    schema_read_csv_funcs(_schemas.schemas)

if config.protocol.url or config.protocol.file:
    from ..util import protocol as _protocol
    armt_read_csv_funcs(_protocol.protocol)
