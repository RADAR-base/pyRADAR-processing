#!/usr/bin/env python3
from typing import Callable, Dict
import dateutil
from datetime import datetime
import numpy as np
import pandas as pd


DT_MULT = int(1e9)


class RadarCsvReader():
    def __init__(self, dtypes=None, column_processors=None,
                 has_array_fields=True, timecols=tuple(),
                 replace_dots=True):
        self.dtypes = dtypes
        self.column_processors = column_processors if column_processors else {}
        self.has_array_fields = has_array_fields
        self.timecols = timecols
        self.replace_dots = replace_dots

    def __call__(self, f):
        df = pd.read_csv(f, dtype=self.dtypes,
                         parse_dates=False, compression='gzip')
        df.columns = ['.'.join(c.split('.')[1:]) for c in df.columns]
        for c, processor in self.column_processors.items():
            df[c] = processor(df[c])
        if self.has_array_fields:
            df = _melt_array_fields(df)
        for c in self.timecols:
            df[c] = date_parser(df[c])
        index = choose_index(df.columns)
        if index:
            df = df.set_index(index)
        if 'userId' in df.columns:
            df['userId'] = df['userId'].apply(lambda x: x[:36])
        if self.replace_dots:
            df.columns = [c.replace('.', '_') for c in df.columns]
        return df


def choose_index(columns):
    for c in ['time', 'dateTime', 'timeReceived']:
        if c in columns:
            return c
    return None


def find_array_columns(columns):
    def is_array_column(c):
        c_split = c.split('.')
        if len(c_split) > 2:
            if c_split[-2].isdigit():
                return True
        return False
    id_vars = []
    arr_vars = []
    for c in columns:
        if is_array_column(c):
            arr_vars.append(c)
        else:
            id_vars.append(c)
    return id_vars, arr_vars


def _melt_array_fields(df):
    def field_name(x):
        x.pop(-2)
        return '.'.join(x)

    def melt_row(row, ids):
        melt = row.melt(id_vars=ids)
        melt['arrid'] = [(lambda x: x[-2])(x)
                         for x in melt['variable'].str.split('.')]
        melt['field'] = [field_name(x)
                         for x in melt['variable'].str.split('.')]
        col_data = [(x[0], x[1]['value'].values)
                    for x in melt.groupby('field')]
        col_data.append(('arrid', melt[melt['field'] == melt['field'][0]]['arrid'].values))
        melt = pd.DataFrame([x[1][ids].iloc[0] for x in melt.groupby('arrid')])
        melt = melt.reset_index(drop=True)
        for col, data in col_data:
            melt[col] = data
        return melt
    id_vars, arr_vars = find_array_columns(df.columns)
    if any(id_vars) and any(arr_vars):
        df = pd.concat([melt_row(df[i:i+1], id_vars)
                        for i in range(len(df))], sort=True)
    return df


def date_parser(series):
    """Convert an array of floats or strings to a datetime object.
    Params:
        series (pd.Series[float/str])
    Returns:
        np.ndarray[M8[ns]] / datetime.datetime
    """
    series = series.values
    if type(series[0]) == float:
        series = series.astype(float)
    if series.dtype == 'float':
        return (series * 1e9).astype('M8[ns]')
    return pd.to_datetime(series)


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
