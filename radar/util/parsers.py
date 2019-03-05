#!/usr/bin/env python3
def biovotion_sort_upsample(df, time_idx='value.time'):
    df = df.sort_index(ascending=False).sort_values(by=time_idx, kind='mergesort')
    lens = df.groupby('value.time').apply(len).values
    deltas = np.array([i/j for j in lens for i in range(j)])
    df[time_idx] = (df[time_idx].astype(int) +
                    deltas.dot(10**9).astype(int)).astype(np.datetime64)
    return df


def timestamp_from_string(stampstr, fromtz='CET', totz='UTC'):
    import pandas as pd
    from datetime import datetime
    try:
        sz_unix = float(stampstr)
        return pd.Timestamp(sz_unix, unit='s').tz_localize('UTC').tz_convert(totz)
    except:
        return pd.Timestamp(datetime.strptime(stampstr, '%Y-%m-%d %H:%M:%S')).tz_localize(fromtz).tz_convert(totz)
