#!/usr/bin/env python3
from typing import Callable, Dict
import dateutil
from datetime import datetime
import numpy as np
import pandas as pd
from ..common import config


DT_MULT = int(1e9)


class RadarCsvReader():
    def __init__(self, dtype=None, column_processors=None,
                 has_array_fields=True):
        self.dtype = dtype
        self.column_processors = column_processors if column_processors else {}
        self.has_array_fields = has_array_fields

    def __call__(self, f):
        df = pd.read_csv(f, dtype=self.dtype, parse_dates=False)
        df.columns = ['.'.join(c.split('.')[1:]) for c in df.columns]
        for c, processor in self.column_processors.items():
            df[c] = processor(df[c])
        if self.has_array_fields:
            df = _melt_array_fields(df)
        index = choose_index(df.columns)
        if index:
            df = df.set_index(index)
        return df


def choose_index(columns):
    for c in ['time', 'dateTime', 'timeReceived']:
        if c in columns:
            return c
    return None


def find_array_columns(columns):
    def is_array_column(c):
        if len(c.split('.')) > 2:
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
    def melt_row(row, ids):
        melt = row.melt(id_vars=ids)
        melt['arrid'] = [(lambda x: x[-2])(x)
                         for x in melt['variable'].str.split('.')]
        melt['field'] = [(lambda x: x[-1])(x)
                         for x in melt['variable'].str.split('.')]
        col_data = [(x[0], x[1]['value'].values)
                    for x in melt.groupby('field')]
        melt = pd.DataFrame([x[1][ids].iloc[0] for x in melt.groupby('arrid')])
        melt = melt.reset_index(drop=True)
        for col, data in col_data:
            melt[col] = data
        return melt
    id_vars, arr_vars = find_array_columns(df.columns)
    if any(id_vars):
        df = pd.concat([melt_row(df[i:i+1], id_vars)
                        for i in range(len(df))], sort=True)
    return df


def date_parser(series):
    """Convert an array of floats or a string to a datetime object.
    Params:
        series (np.ndarray[float], str)
    Returns:
        np.ndarray[M8[ns]] / datetime.datetime
    """
    if isinstance(series, np.ndarray):
        ts = (series * 1e9).astype('M8[ns]')
    elif isinstance(series, np.float64):
        ts = datetime.fromtimestamp(series)
    elif isinstance(series, str):
        ts = dateutil.parser.parse(series)
    return ts


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
