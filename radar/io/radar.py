#!/usr/bin/env python3
import glob
import pandas as pd
import dask.delayed as delayed
import dask.dataframe as dd
from ..common import config
from .generic import _data_load_funcs

def read_csv(path, *args, **kwargs):
    files = glob.glob(path + '/*.csv*')
    files.sort()
    dtype = kwargs.get('dtype', {})
    timecols = kwargs.get('timecols', [])
    index = kwargs.get('index', config.io.index)
    divisions = [file_datehour(fn) for fn in files] +\
                [file_datehour(files[-1]) + pd.Timedelta(1, 'h')]
    delayed_files = [delayed(_read_csv_func(dtype=dtype, timecols=timecols,
                                            index=index))(fn) for fn in files]
    df = dd.from_delayed(delayed_files, divisions=divisions)
    return df

def _read_csv_func(dtype=None, timecols=None, index=config.io.index):
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

def file_datehour(fn):
    return pd.Timestamp(fn.split('/')[-1].split('.')[0].replace('_', 'T'))

def read_csv_schema(schema, *args, **kwargs):
    dtype = schema.dtype()
    timecols = schema.timecols()
    def delayed_read_csv(path):
        df = read_csv(path, dtype=dtype, timecols=timecols, *args, **kwargs)
        return df
    return delayed_read_csv

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
        print(name)
        _data_load_funcs[name] = read_csv_schema(scm)

if config.schema.read_csvs:
    from ..util import schemas
    schema_read_csv_funcs(schemas.schemas)
