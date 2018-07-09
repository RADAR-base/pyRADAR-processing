#!/usr/bin/env python3
import h5py
import pandas as pd
import dask.array as da
import dask.dataframe as dd
from dask.bytes.utils import infer_storage_options
from .core import get_fs

class Imec(object):
    def __init__(self, path):
        fs = get_fs(**infer_storage_options(path))
        self._h5s = [h5py.File(h5) for h5 in
                     fs.glob(path + fs.sep + '*' + fs.sep + '*.h5')]
        self._start_times = []
        for h5 in self._h5s:
            start_time = h5['/Devices/Radar'].attrs.get('#DateTime')
            if start_time is None:
                raise ValueError(('The given IMEC h5 file does not have '
                                  'a date/time attribute. Please ensure it '
                                  'was converted correctly'))
            else:
                self._start_times.append(
                        pd.Timestamp(bytes.decode(start_time)))

        self._dsets = {
                k: [h5['/Devices/Radar/Signal/{}/Data'.format(k)]
                    for h5 in self._h5s]
                for k in self._h5s[0]['/Devices/Radar/Signal/']
                }

        self._signals = {
                k: da.stack(
                    [da.from_array(dset, chunks=self._signal_chunks(k))
                        for dset in self._dsets[k]], axis=0).transpose()
                    for k, v in self._dsets.items()
                }

        self._timecols = {
                'accelerometer':self._make_timecols('ACC-X'),
                'electrode' : self._make_timecols('ECG'),
                'other': self._make_timecols('Battery'),
                }
        self.imec_acceleration = \
                self._df_from_signals(['ACC-X', 'ACC-Y', 'ACC-Z'],
                                      ['x', 'y', 'z'],
                                      'accelerometer')
        self.imec_gsr = \
                self._df_from_signals(['GSR-1', 'GSR-2'],
                                      ['GSR_1', 'GSR_2'],
                                      'electrode')
        self.imec_ecg = \
            self._df_from_signals(['ECG'], ['ECG'], 'electrode')
        self.imec_emg = \
            self._df_from_signals(['EMG'], ['EMG'], 'electrode')
        self.imec_battery = \
            self._df_from_signals(['Battery'], ['battery'], 'other')
        self.imec_temperature = \
            self._df_from_signals(['Temp'], ['temperature'], 'other')
        self.imec_pie = \
            self._df_from_signals(['PIE'], ['PIE'], 'other')

    def _df_from_signals(self, signals, names, timecol):
        arrs = [self._signals[sig] for sig in signals]
        catarr = da.concatenate(arrs, axis=1)
        df = dd.from_dask_array(catarr, columns=names)
        # df['time'] = dd.from_dask_array(self._timecols[timecol])
        return df

    def _signal_freq(self, key):
        h5 = self._h5s[0]
        return float(h5['/Devices/Radar/Signal/{}'.format(key)].attrs['#Freq'])

    def _signal_chunks(self, key):
        freq = self._signal_freq(key)
        return (60*60*2*freq,)

    def _make_timecols(self, signal):
        chunks = self._signal_chunks(signal)
        freq = self._signal_freq(signal)
        lengths = [len(x) for x in self._dsets[signal]]
        arrs = [da_date_idx(start, N, freq, chunks)
                for start, N in zip(self._start_times, lengths)]
        return da.stack(arrs, axis=0)

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
