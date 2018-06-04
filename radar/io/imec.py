#!/usr/bin/env python3
import tables
import numpy as np
import pandas as pd
import dask.array as da
import matplotlib.pyplot as plt

class Imec(object):
    def __init__(self, imec_file_path):
        self._h5 = tables.File(imec_file_path)
        self._radar = self._h5.root.Devices.Radar

        if hasattr(self._radar._v_attrs, '#DateTime'):
            self._start_time = pd.Timestamp(bytes.decode(
                getattr(self._radar._v_attrs, '#DateTime')))
        else:
            raise ValueError('The given IMEC file does not have a date/time')
        self._h5arrs = {k: getattr(self._radar.Signal, k).Data
                        for k in self._radar.Signal._v_children}
        self.modalities = {
                'Accelerometer': ['ACC-X', 'ACC-Y', 'ACC-Z'],
                'Battery': ['Battery'],
                'ECG': ['ECG'],
                'EMG': ['EMG'],
                'GSR': ['GSR-1', 'GSR-2'],
                'PIE': ['PIE'],
                'Temp': ['Temp'],
                }
        self._freqs = {}
        self._timecols = {}
        for modal, arrs in self.modalities.items():
            self._timecols[modal] = self._make_timecol(arrs[0])
            self._freqs[modal] = self.signal_freq(arrs[0])

    def signal_freq(self, key):
        return float(getattr(getattr(self._radar.Signal, key)._v_attrs,
                             '#Freq'))

    def _make_timecol(self, arr):
        col_len = len(self._h5arrs[arr])
        col_freq = self.signal_freq(arr)
        chunks = self._h5arrs[arr].chunkshape
        col = da_date_idx(self._start_time, col_len, col_freq, chunks)
        return col

    def get_df(self, modality, index):
        cols = {'time': self._timecols[modality][index].compute()}
        cols.update({name: self._h5arrs[name][index]
                     for name in self.modalities[modality]})
        return pd.DataFrame(cols)

    def get_df_time(self, modality, start_time, stop_time, sample_rate=None):
        dateidx = DateToIdx(self._start_time, self._freqs[modality])
        start_idx = dateidx(start_time)
        start_idx = 0 if start_idx < 0 else start_idx
        stop_idx = dateidx(stop_time)
        index = slice(start_idx, stop_idx, None)
        df = self.get_df(modality, index)
        df.set_index('time', inplace=True)
        if sample_rate is not None:
            df = df.resample(sample_rate).pad()
        return df

    def plot_timespan(self, modality, start_time, stop_time, sample_rate=None):
        df = self.get_df_time(modality, start_time, stop_time, sample_rate)
        fig = plt.figure()
        plt.plot(df)
        plt.legend(df.columns)
        return fig


class IdxToDate():
    def __init__(self, start, freq):
        self.start = pd.Timestamp(start, 'ns').asm8.astype('int64')
        self.freq = freq
    def __call__(self, x):
        return (self.start + (int(1e9 / self.freq) * x)).astype('M8[ns]')


class DateToIdx():
    def __init__(self, start, freq):
        self.start = pd.Timestamp(start, 'ns').asm8.astype('int64')
        self.freq = freq

    def __call__(self, date):
        date = pd.Timestamp(date, 'ns').asm8.astype('int64')
        idx = (((date - self.start) / 1e9) * self.freq)
        return int(round(idx))

def da_date_idx(start, N, freq, chunks):
    return da.fromfunction(IdxToDate(start, freq), shape=(N,),
                           chunks=chunks, dtype='M8[ns]')
