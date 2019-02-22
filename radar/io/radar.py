#!/usr/bin/env python3
import glob
import base64
from functools import wraps
from typing import Callable, Dict

import numpy as np
import pandas as pd
import dask.delayed as delayed
import dask.dataframe as dd
from .generic import _data_load_funcs
from ..common import config
from ..util.armt import melt

DT_MULT = int(1e9)


def file_datehour(fn):
    return pd.Timestamp(fn.split('/')[-1][0:13]
                        .replace('_', 'T')).tz_localize('UTC')


def read_csv_folder(func):
    @wraps(func)
    def wrapper(path, *args, **kwargs):
        files = glob.glob(path + '/*.csv*')
        files.sort()
        try:
            divisions = [file_datehour(fn) for fn in files] +\
                        [file_datehour(files[-1]) + pd.Timedelta(1, 'h')]
        except ValueError:
            divisions = None
        delayed_files = [delayed(func)(fn, *args, **kwargs)
                         for fn in files]
        df = dd.from_delayed(delayed_files, divisions=divisions)
        if df.divisions == (None, None):
            df.divisions = (df.index.head()[0], df.tail().index[-1])
        return df
    return wrapper


def read_prmt_csv(dtype=None, timecols=None,
                  timedeltas=None, index=config['io']['index']):
    if dtype is None:
        dtype = {}
    if timecols is None:
        timecols = []
    if timedeltas is None:
        timedeltas = {}
    dtype['key.projectId'] = object

    @read_csv_folder
    def read_csv(path, *args, **kwargs):
        df = pd.read_csv(path, *args, dtype=dtype, **kwargs)
        for col in timecols:
            if df[col].dtype == 'int' or df[col].dtype == 'float':
                df[col] = pd.DatetimeIndex(
                    (DT_MULT * df[col].values).astype('Int64'),
                    tz='UTC')
            else:
                df[col] = pd.DatetimeIndex(pd.to_datetime(df[col]))

        for col, deltaunit in timedeltas.items():
            df[col] = df[col].astype('Int64').astype(deltaunit)

        df.columns = [c.split('.')[-1] for c in df.columns]
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


if config['schema']['read_csvs']:
    from ..util import schemas as _schemas
    _data_load_funcs.update(schema_read_csv_funcs(_schemas.schemas))

if config['protocol']['url'] or config['protocol']['file']:
    from ..util import protocol as _protocol
    _data_load_funcs.update(armt_read_csv_funcs(_protocol.protocols))


# Fitbit temp
_data_load_funcs['connect_fitbit_intraday_steps'] = read_prmt_csv(
                     timecols=['value.time', 'value.timeReceived'],
                     timedeltas={'value.timeInterval': 'timedelta64[s]'})

_data_load_funcs['connect_fitbit_intraday_heart_rate'] = read_prmt_csv(
                     timecols=['value.time', 'value.timeReceived'],
                     timedeltas={'value.timeInterval': 'timedelta64[s]'})

_data_load_funcs['connect_fitbit_sleep_stages'] = read_prmt_csv(
    timecols=['value.dateTime', 'value.timeReceived'],
    timedeltas={'value.duration': 'timedelta64[s]'},
    index='dateTime')

_data_load_funcs['connect_fitbit_sleep_classic'] = read_prmt_csv(
    timecols=['value.dateTime', 'value.timeReceived'],
    timedeltas={'value.duration': 'timedelta64[s]'},
    index='dateTime')


_data_load_funcs['connect_fitbit_time_zone'] = read_prmt_csv(
    timecols=['value.timeReceived'],
    index='timeReceived')


def read_processed_audio():
    def convert_data(x):
        return np.array(base64.b64decode(''.join(x.split('\\n')))
                .split(b'\n')[1].split(b';')[1:], dtype=float)

    def convert_data_delayed(series):
        return series.map(convert_data)

    def read_csv(path, *args, **kwargs):
        df = delayed_read(path, *args, **kwargs)
        df = df.dropna()
        df['data'] = df['data'].map_partitions(
             convert_data_delayed, meta=('data', object))
        return df

    delayed_read = read_prmt_csv(timecols=['value.time', 'value.timeReceived'],
                                 dtype={'value.data': object})
    return read_csv


_data_load_funcs['android_processed_audio'] = read_processed_audio()
