#!/usr/bin/env python3
import h5py
import dask.array as da
import dask.dataframe as dd
from dask.bytes.utils import infer_storage_options
from .core import get_fs
from .generic import FakeDatetimeArray

class Imec(dict):
    def __init__(self, path):
        fs = get_fs(**infer_storage_options(path))
        if fs.isfile(path):
            paths = [path]
        else:
            paths = fs.glob(path + fs.sep + '*.h5')

        self._h5s = {p: h5py.File(p) for p in paths}
        self._start_times = [self._h5_start_time(h5) for h5 in h5s.values()]


    @staticmethod
    def _h5_start_time(h5):
        """ Function to return the start DateTime attribute from a radar IMEC
        hdf5 file, opened with h5py.

        Parameters
        __________
        h5 : h5py File
            A RADAR-EPI IMEC hdf5 file opened with h5py

        Returns
        _______
        start_time : str
            A string representation of the start datetime

        """
        start_time = h5['/Devices/Radar/'].attrs.get('#DateTime')
        if start_time is None:
            raise ValueError(('The IMEC file "{}" does not have a datetime '
                              'attribute. Please ensure it was converted'
                              'using a working version of nyx_legacy'\
                              .format(h5.id.name.decode())))
        return start_time.decode()

    def _dfs_from_h5(self, h5):
        start_time = self._h5_start_time(h5)




