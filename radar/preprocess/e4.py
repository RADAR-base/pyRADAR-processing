#!/usr/bin/env python3
import numpy as np
import pandas as pd

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

def consecutive(data, stepsize=1):
    """ Splits an array into a list of consecutive arrays
    Parameters
    __________
    data: List/array
    stepsize: int
        The distance between consecutive items (default 1)

    Returns
    _______
    list
        List of np.ndarray
    """
    return np.split(data, np.where(np.diff(data) != stepsize)[0]+1)

def drift_filter(delta):
    """ Filters the time drift series
    Parameters
    __________
    delta: int64 pd.Series with timestamp index
    Returns
    _______
    filt: float64 pd.Series with timestamp index
        A 'low-pass' filtered series with jumps between linear segments = NaN
    """
    def split_peaks(filt):
        filt[filt.diff(5) < -1e6] = np.nan
        filt = filt.fillna(method='bfill')
        filt[filt.diff() < -5e7] = np.nan
        return filt

    def find_starts(filt):
        starts = filt.index[np.logical_and(filt.diff() > 1e7, filt > 0)]
        return starts

    def fix_starts(filt, starts):
        for s in starts:
            filt.loc[s:s + (2*SEC)] = filt[s + (2*SEC):s + (3*SEC)].max()
            filt.loc[s] = np.nan
        return filt

    filt = delta.rolling('20s').min()
    # filt = split_peaks(filt)
    starts = find_starts(filt)
    filt = fix_starts(filt, starts)
    return filt

def get_segments(series):
    """ Get contiguous non-NaN segments
    Parameters
    __________
    series: pd.Series
    Returns
    _______
    segments: list
    """
    nans = consecutive(np.where(series.isna())[0])
    if len(nans[0]) == 0:
        return [(0, len(series))]
    segments = [(0, nans[0][0])]
    for i in range(len(nans)-1):
        segments.append((nans[i][-1] + 1, nans[i+1][0]))
    if not np.any(np.isnan(series.iloc[-1])):
        segments.append((nans[-1][-1] + 1, len(series)))
    return segments

def lm_segments(series, segments):
    """ Fit linear models to each segment
    """
    def fit_lm(series):
        x = series.index.values.astype('int64')
        y = series.values
        fit = np.polyfit(x, y, 1)
        return np.poly1d(fit)

    # zero = np.max([series.iloc[seg[0]] for seg in segments])
    # zero = np.max(zero, 0)
    fits = {'time': [0] * len(segments),
            'fit': [0] * len(segments)}
    for i, ind in enumerate(segments):
        seg = series.iloc[slice(*ind)]
        fits['time'][i] = seg.index[0]
        fits['fit'][i] = fit_lm(seg)
    return pd.DataFrame(data=fits).set_index('time')

def drift_fits(df):
    delta = calculate_drift(df['timeReceived'])
    filt = drift_filter(delta)
    segments = get_segments(filt)
    fits = lm_segments(filt, segments)
    return fits

def correct_drift(df, fits, inplace=True):
    if not inplace:
        df = df.copy()
    tz = df.index.tz
    new_time = df.index.to_series()
    for i in range(len(fits)-1):
        start = fits.index[i] - 0.1*SEC
        stop = fits.index[i+1]
        p = fits.iloc[i]['fit']
        new_time[start:stop] += p(new_time[start:stop].index\
                .astype('int64'))\
                .astype('timedelta64[ns]')
    start = fits.index[-1]
    p = fits.iloc[-1]['fit']
    new_time[start:] += p(new_time[start:].index\
            .astype('int64'))\
            .astype('timedelta64[ns]')
    df.index = new_time.values
    df.index = df.index.tz_localize(tz)
    return
