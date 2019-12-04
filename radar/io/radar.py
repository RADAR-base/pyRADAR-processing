#!/usr/bin/env python3
from functools import wraps
from typing import Callable, Dict
import pandas as pd
import pyarrow.csv as pcsv
import dask.delayed as delayed
import dask.dataframe as dd
from .core import glob_path_for_files
from .generic import create_divisions
from ..common import config
from ..util.armt import melt

DT_MULT = int(1e9)


class RadarCsvReader():
    def __init__(self, dtype=None, timecols=None,
                 timedeltas=None, index=config['io']['index']):
        self.dtype = dtype if dtype else {}
        self.timecols = timecols if timecols else {}
        self.timedeltas = timedeltas if timedeltas else {}
        self.index = index

    def __call__(self, path, **kwargs):
        files = sorted(glob_path_for_files(path, '*.csv*'))
        if not files:
            return None
        divisions = create_divisions(files)
        delayed_files = [delayed(self.read_csv)(fn, **kwargs)
                         for fn in files]
        df = dd.from_delayed(delayed_files, divisions=divisions)
        if df.divisions == (None, None):
            df.divisions = (df.index.head()[0], df.tail().index[-1])
        return df

    def read_csv(self, p, *args, **kwargs):
        data = pd.read_csv(p, dtype=self.dtype, **kwargs)
        return data


class PrmtCsvReader(RadarCsvReader):
    drop_duplicates = True
    rename_columns = True
    drop_unspecified = True

    def read_csv(self, p, *args, **kwargs):
        df = pd.read_csv(p, dtype=self.dtype)
        df[self.timecols] = df[self.timecols].apply(convert_to_datetime)
        for col, deltaunit in self.timedeltas.items():
            df[col] = df[col].astype('Int64').astype(deltaunit)
        if self.drop_unspecified:
            extracols = [c for c in df.columns
                         if c not in self.dtype and c[:3] != 'key']
            df = df.drop(columns=extracols)
        if self.rename_columns:
            df.columns = [c.split('.')[-1] for c in df.columns]
        if self.drop_duplicates:
            df = df.drop_duplicates(self.index)
        if self.index:
            df = df.set_index(self.index)
            df = df.sort_index()
        return df


class ArmtCsvReader(RadarCsvReader):
    def read_csv(self, path, *args, **kwargs):
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
        df = df.set_index(self.index)
        df = df.sort_index(kind='mergesort')
        df = df[['projectId', 'sourceId', 'userId',
                 'questionId', 'startTime', 'endTime',
                 'value', 'name', 'timeCompleted',
                 'timeNotification', 'version', 'arrid']]
        return df


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
        out[name] = PrmtCsvReader(scm.dtype, scm.timecols, scm.timedeltas)
    return out


def armt_read_csv_funcs(protocol) -> Dict[str, Callable]:
    out = {}
    for armt in protocol.values():
        name = armt.questionnaire.avsc + '_' + armt.questionnaire.name
        out[name] = ArmtCsvReader()
    return out
