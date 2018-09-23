#!/usr/bin/env python3
import h5py
import numpy as np
import pandas as pd
import dask.array as da
import dask.dataframe as dd
from dask.bytes.utils import infer_storage_options
from .core import get_fs
from .generic import FakeDatetimeArray

class Imec(dict):
    def __init__(self, path):
        fs = get_fs(**infer_storage_options(path))
        if fs.isfile(path):
            h5_paths = [path]
        else:
            h5_paths = fs.glob(path + fs.sep + '*.h5')

        h5s = [h5py.File(p, mode='r') for p in h5_paths]
        start_times = [h5['/Devices/Radar/'].attrs.get('#DateTime')
                       for h5 in h5s]
        for i, st in enumerate(start_times):
            if st is None:
                raise ValueError(('The IMEC file "{}" does not have a date/time '
                                  'attribute. Please ensure it was converted'
                                  'properly'.format(h5_paths[i])))
            else:
                start_times[i] = st.decode()

        self._h5s = h5s
        self._start_times = start_times
        self._dsets = {k: [h5['/Devices/Radar/Signal/{}/Data'.format(k)]
                           for h5 in self._h5s]
                       for k in self._h5s[0]['/Devices/Radar/Signal/']}
        self._signals = {
            k: da.concatenate(
                [da.from_array(dset, chunks=self._signal_chunks(k))
                 for dset in self._dsets[k]],
                axis=0)
            for k in self._dsets}

        self._timecols = {signal: self._make_timecol(signal)
                          for signal in ('ECG', 'ACC-X', 'Temp')}

        self['imec_acceleration'] =\
                self._da_from_signals(('ACC-X', 'ACC-Y', 'ACC-Z'),
                                      ('x', 'y', 'z'),
                                      self._timecols['ACC-X'])
        self['imec_gsr'] =\
                self._da_from_signals(('GSR-1', 'GSR-2'),
                                      ('GSR_1', 'GSR_2'),
                                      self._timecols['ECG'])
        self['imec_ecg'] =\
                self._da_from_signals(('ECG',),
                                      ('ECG',),
                                      self._timecols['ECG'])
        self['imec_emg'] =\
                self._da_from_signals(('EMG',),
                                      ('EMG',),
                                      self._timecols['ECG'])
        self['imec_temperature'] =\
                self._da_from_signals(('Temp',),
                                      ('temp',),
                                      self._timecols['Temp'])
        self['imec_pie'] =\
                self._da_from_signals(('PIE',),
                                      ('PIE',),
                                      self._timecols['Temp'])

    def _signal_chunks(self, key):
        freq = self._signal_freq(key)
        return int(60*60*freq)

    def _signal_freq(self, key):
        h5 = self._h5s[0]
        return float(h5['/Devices/Radar/Signal/{}'.format(key)].attrs['#Freq'])

    def _make_timecol(self, signal):
        freq = self._signal_freq(signal)
        lengths = [len(x) for x in self._dsets[signal]]
        chunks = self._signal_chunks(signal)
        clen = 0
        arrs = []
        divs = []
        for i in range(len(lengths)):
            N = lengths[i]
            start = self._start_times[i]
            arr = FakeDatetimeArray(start, N, freq)
            arrs.append(da.from_array(arr, chunks=chunks))
            chunk_array = np.zeros(1 + len(arrs[i].chunks[0]))
            chunk_array[1:] = np.cumsum(arrs[i].chunks[0])
            divs.extend(arr[chunk_array])
        divs.append(arr[len(arr) - 1])
        return {'timecol': da.concatenate(arrs, axis=0),
                'divs': divs}

    def _da_from_signals(self, signals, names, timecol):
        arrs = [self._signals[sig] for sig in signals]
        catarr = da.stack(arrs, axis=1)
        df = dd.from_dask_array(catarr, columns=names)
        df['time'] = dd.from_dask_array(timecol['timecol'])
        df = df.set_index('time', sorted=True, divisions=timecol['divs'])
        return df
