#!/usr/bin/env python3
def biovotion_sort_upsample(df, time_idx='value.time'):
    df = df.sort_index(ascending=False).sort_values(by=time_idx, kind='mergesort')
    lens = df.groupby('value.time').apply(len).values
    deltas = np.array([i/j for j in lens for i in range(j)])
    df[time_idx] = (df[time_idx].astype(int) +
                    deltas.dot(10**9).astype(int)).astype(np.datetime64)
    return df

def biovotion_preprocessing(df, time_idx='time'):
    import numpy as np
    import dask.dataframe as dd
    import pandas as pd
    if isinstance(df, dd.DataFrame): df = df.compute()
    df = df.sort_index(ascending=False).sort_values(by=time_idx, kind='mergesort')
    lens = df.groupby(time_idx).apply(len).values
    deltas = np.array([i/j for j in lens for i in range(j)])
    #df[time_idx] = (df[time_idx].astype(int) + deltas.dot(10**9).astype(int)).astype(np.datetime64)
    df[time_idx] = pd.to_datetime(df[time_idx].astype(int) + deltas.dot(10**9).astype(int))
    df = df.set_index(time_idx)
    return df

def timestamp_from_string(stampstr, stringfmt="%Y-%m-%d %H:%M:%S", fromtz='CET', totz='UTC'):
    import pandas as pd
    from datetime import datetime
    try:
        sz_unix = float(stampstr)
        return pd.Timestamp(sz_unix, unit='s').tz_localize('UTC').tz_convert(totz)
    except:
        return pd.Timestamp(datetime.strptime(stampstr, stringfmt)).tz_localize(fromtz).tz_convert(totz)
