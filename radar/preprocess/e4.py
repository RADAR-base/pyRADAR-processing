#!/usr/bin/env python3
import numpy as np
import pandas as pd
from .filters import butterworth

SEC = pd.Timedelta(1, 's')

def calculate_drift(timeReceived):
    """ Calculate timedelta between timeRecieved and time.
    Parameters
    __________
    timeReceived: pandas.Series
        A pandas timestamp series with a timestamp index.

    Returns
    ________
    drift: pandas.Series
        A pandas int64 series showing the difference (ns)
        between index and input series.
    """
    return (timeReceived - timeReceived.index).astype('int64')


def drift_filter(delta, freq=32):
    """ Filters the time drift series
    Finds the minimum value in rolling 120s sections.
    Params:
        delta (pd.Series[int64]): Series with timestamp index
    Returns:
        pd.Series[float64]: filtered drift
    """

    filt = delta.rolling('120s').min()
    start = filt.index[0]
    filt.loc[start:start+120*SEC] = filt.loc[start:start+120*SEC].min()
    return filt


def get_segments(series):
    """ Get contiguous segments. Splits on gaps in the time index > 0.5sec
    Params:
        series (pd.Series)
    Returns:
        list: List of tuples (Segment indices)
    """
    def split_gaps(series):
        arr = series.index.astype(int)
        deriv = np.append(0, np.diff(arr))
        return np.where(np.abs(deriv) > 5e8)[0].tolist()

    gaps = split_gaps(series)
    gaps.append(len(series))
    segments = [(0, gaps[0])]
    for i in range(0, len(gaps)-1):
        segments.append((gaps[i], gaps[i+1]))
    return segments


def filter_segments(series, segments):
    out = series.copy()
    for i, ind in enumerate(segments):
        seg = series.iloc[slice(*ind)]
        out.iloc[slice(*ind)] = drift_filter(seg)
    return out


def lm_segments(series, segments):
    """ Fit linear models to each segment
    Currently unused
    """
    def fit_lm(series):
        x = series.index.values.astype('int64')
        y = series.values
        fit = np.polyfit(x, y, 1)
        return np.poly1d(fit)

    # zero = np.max([series.iloc[seg[0]] for seg in segments])
    # zero = np.max(zero, 0)
    fits = {'time': [0] * len(segments),
            'fit': [0] * len(segments)}
    for i, ind in enumerate(segments):
        seg = series.iloc[slice(*ind)]
        fits['time'][i] = seg.index[0]
        fits['fit'][i] = fit_lm(seg)
    return pd.DataFrame(data=fits).set_index('time')


def correct_drift(df, inplace=True):
    if not inplace:
        df = df.copy()
    drift = calculate_drift(df['timeReceived'])
    segments = get_segments(drift)
    filtered = filter_segments(drift, segments)
    df.index = df.index + filtered.astype('timedelta64[ns]')
    return df
