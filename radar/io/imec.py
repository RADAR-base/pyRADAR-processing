#!/usr/bin/env python3
from typing import List
from functools import partial
import h5py
import pandas as pd
import dask.array as da
import dask.dataframe as dd
from .core import glob_path_for_files, terminal_folders
from .generic import FakeDatetimeArray


class ImecFile():
    def __init__(self, path: str, signals: List[str], offset: int = 0):
        self.signals = signals
        self.h5 = h5py.File(path, mode='r')
        assert self._unique_frequencies == 1
        self.offset = offset
        self._datetime = pd.Timestamp(
            self.h5['Devices/Radar'].attrs['#DateTime'].decode())
        self.time_array = FakeDatetimeArray(self.start_time, len(self),
                                            freq=self.freq)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.h5.close()

    def __del__(self):
        self.h5.close()

    def __len__(self):
        return len(self.arrays[0]) - self.offset

    @staticmethod
    def h5key(signal):
        return 'Devices/Radar/Signal/' + signal

    @property
    def _unique_frequencies(self):
        def get_freq(signal):
            return self.h5[self.h5key(signal)].attrs['#Freq']
        freqs = set([get_freq(signal) for signal in self.signals])
        return len(freqs)

    @property
    def freq(self):
        key = self.h5key(self.signals[0])
        return int(self.h5[key].attrs['#Freq'])

    @property
    def si_unit(self):
        key = self.h5key(self.signals[0])
        return self.h5[key].attrs['#SI'].decode()

    @property
    def start_time(self):
        return self._datetime + (self.offset * pd.Timedelta(1e9/self.freq))

    @property
    def arrays(self):
        return [self.h5[self.h5key(s) + '/Data'] for s in self.signals]

    @property
    def chunks(self):
        return self.arrays[0].chunks

    @property
    def dask_chunks(self):
        # 8388608 = 32mb int64
        return self.chunks[0] * (8388608 // self.chunks[0])

    @property
    def dask_time(self):
        return da.from_array(self.time_array, chunks=self.dask_chunks)

    def _dask_array(self, i):
        return da.from_array(self.arrays[i], chunks=self.dask_chunks)[self.offset:]

    @property
    def dask_dataframe(self):
        df = dd.concat([dd.from_dask_array(self.dask_time,
                                           columns=['time'])] +
                       [dd.from_dask_array(self._dask_array(i),
                                           columns=self.signals[i])
                        for i in range(len(self.signals))],
                       axis=1)
        divisions = self.time_array[list(df.divisions)].tolist()
        return df.set_index('time', sorted=True, divisions=divisions)


class ImecContiguous():
    def __init__(self, path: str, signals: List[str], offset: int = 0):
        h5s = sorted(glob_path_for_files(path, '*.h5'),
                     key=lambda x: int(x.split('/')[-1].split('.')[0][4:]))
        self.files = []
        for fn in h5s:
            imf = ImecFile(fn, signals, offset)
            self.files.append(imf)
            offset += len(imf)

    @property
    def dask_dataframe(self):
        return dd.concat([imf.dask_dataframe for imf in self.files])


class ImecDataset():
    def __init__(self, path: str, signals: List[str]):
        folders = terminal_folders(path)
        self._ctg_grps = [ImecContiguous(p, signals) for p in folders]

    @property
    def dask_dataframe(self):
        return dd.concat([cg.dask_dataframe for cg in self._ctg_grps])


class ImecOldDataset():
    def __init__(self, path: str, signals: List[str]):
        imec_files = [fp for p in terminal_folders(path)
                      for fp in glob_path_for_files(p, '*.h5')]
        self._imec_files = [ImecFile(f, signals) for f in imec_files]

    @property
    def dask_dataframe(self):
        return dd.concat([im.dask_dataframe for im in self._imec_files])


def imec_dataset(path: str, signals: List[str]):
    ids = ImecDataset(path, signals)
    ddf = ids.dask_dataframe
    ddf.imec_dataset = ids
    return ddf


def imec_single(path: str, signals: List[str]):
    imec = ImecFile(path, signals)
    ddf = imec.dask_dataframe
    ddf.imec_file = imec
    return ddf


def imec_old(path: str, signals: List[str]):
    ids = ImecOldDataset(path, signals)
    ddf = ids.dask_dataframe
    ddf.imec_dataset = ids
    return ddf


imec_acceleration = partial(imec_dataset, signals=('ACC-X', 'ACC-Y', 'ACC-Z'))
imec_ecg = partial(imec_dataset, signals=('ECG',))
imec_emg = partial(imec_dataset, signals=('EMG',))
imec_gsr = partial(imec_dataset, signals=('GSR-1', 'GSR-2'))


def imec_h5_all(path):
    out = {
        'imec_acceleration': imec_acceleration(path),
        'imec_ecg': imec_ecg(path),
        'imec_emg': imec_emg(path),
        'imec_gsr': imec_gsr(path)
    }
    return out


def imec_old_all(path):
    out = {
        'imec_old_acceleration': imec_old(path, signals=('ACC-X',
                                                         'ACC-Y',
                                                         'ACC-Z')),
        'imec_old_ecg': imec_old(path, signals=('ECG',)),
        'imec_old_emg': imec_emg(path, signals=('EMG',)),
        'imec_old_gsr': imec_gsr(path, signals=('GSR-1', 'GSR-2'))
    }
    return out
