#!/usr/bin/env python3
from functools import wraps
from typing import Callable, Dict
import pandas as pd
import dask.delayed as delayed
import dask.dataframe as dd
from .core import glob_path_for_files, files_newer_than
from .generic import create_divisions
from ..common import config
from ..util.armt import melt

DT_MULT = int(1e9)


def is_numeric(series):
    """ Checks whether pandas series dtype is a float or integer.
    Params:
        series (pd.Series): Pandas series to check
    Returns:
        bool
    """
    return series.dtype == 'float' or series.dtype == 'int'


def convert_to_datetime(series):
    if is_numeric(series):
        return pd.DatetimeIndex(DT_MULT * series, tz='UTC')
    return pd.DatetimeIndex(pd.to_datetime(series))


def read_csv_folder(func):
    @wraps(func)
    def wrapper(path, *args, **kwargs):
        files = glob_path_for_files(path, '*.csv*')
        files.sort()
        newer_than = kwargs.pop('files_newer_than', None)
        if newer_than:
            files = files_newer_than(files, newer_than)
        if not files:
            return None
        divisions = create_divisions(files)
        delayed_files = [delayed(func)(fn, *args, **kwargs)
                         for fn in files]
        df = dd.from_delayed(delayed_files, divisions=divisions)
        if df.divisions == (None, None):
            df.divisions = (df.index.head()[0], df.tail().index[-1])
        return df
    return wrapper


def read_prmt_csv(dtype=None, timecols=None,
                  timedeltas=None, index=config['io']['index'],
                  drop_duplicates=True, drop_unspecified=True,
                  rename_columns=True):
    if dtype is None:
        dtype = {}
    if timecols is None:
        timecols = []
    if timedeltas is None:
        timedeltas = {}

    dtype['key.projectId'] = 'category'
    dtype['key.userId'] = 'category'

    @read_csv_folder
    def read_csv(path, *args, **kwargs):
        df = pd.read_csv(path, *args, dtype=dtype, **kwargs)
        df[timecols] = df[timecols].apply(convert_to_datetime)
        for col, deltaunit in timedeltas.items():
            df[col] = df[col].astype('Int64').astype(deltaunit)
        if drop_unspecified:
            extracols = [c for c in df.columns
                         if c not in dtype and c[:3] != 'key']
            df = df.drop(columns=extracols)
        if rename_columns:
            df.columns = [c.split('.')[-1] for c in df.columns]
        if drop_duplicates:
            df = df.drop_duplicates(index)
        if index:
            df = df.set_index(index)
            df = df.sort_index()
        return df
    return read_csv


def read_armt_csv(index=config['io']['index']):
    @read_csv_folder
    def read_csv(path, *args, **kwargs):
        df = pd.read_csv(path, dtype=object, *args, **kwargs)
        df = df.drop_duplicates('value.time')
        df = melt(df)
        if 'value.timeNotification' not in df:
            df['value.timeNotification'] = pd.np.NaN
        for col in ('value.time', 'value.timeCompleted',
                    'startTime', 'endTime', 'value.timeNotification'):
            df[col] = pd.DatetimeIndex((DT_MULT * df[col].astype('float')),
                                       tz='UTC')
        df['arrid'] = df.index
        if 'questionId' not in df:
            df['questionId'] = ''
        df.columns = [c.split('.')[-1] for c in df.columns]
        df = df.set_index(index)
        df = df.sort_index(kind='mergesort')
        df = df[['projectId', 'sourceId', 'userId',
                 'questionId', 'startTime', 'endTime',
                 'value', 'name', 'timeCompleted',
                 'timeNotification', 'version', 'arrid']]
        return df
    return read_csv


def schema_read_csv_funcs(schemas):
    """
    Adds read_csv functions for each schema name to the _data_load_funcs
    in radar.io.generic
    Args:
        schemas (dict): Dictionary containing keys of schema names
            with RadarSchema values
    """
    out = {}
    for name, scm in schemas.items():
        out[name] = read_prmt_csv(scm.dtype(), scm.timecols())
    return out


def armt_read_csv_funcs(protocol) -> Dict[str, Callable]:
    out = {}
    for armt in protocol.values():
        name = armt.questionnaire.avsc + '_' + armt.questionnaire.name
        out[name] = read_armt_csv()
    return out
