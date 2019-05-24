#!/usr/bin/env python3
""" Generic IO functions
"""
from typing import List, Union
import os
import numpy as np
import pandas as pd
from ..generic import methdispatch


class FakeDatetimeArray():
    """ A fake numpy datetime array
    """
    ndim = 1
    _index_error = (
        'Only integer, slices (`:`), ellipsis (`...`), '
        ' integer arrays or boolean arrays are valid indices')

    def __init__(self, start: np.datetime64, length: int,
                 freq: Union[int, float, None] = None,
                 step: Union[int, float, None] = None):
        """
        step : int
            step size in seconds (1/freq)

        negative slices don't work
        """
        if step is None and freq is None:
            raise ValueError('Must provide either freq or step')
        if step:
            self.step = int(1e9) * step
            self.freq = 1/step
        else:
            self.freq = freq
            self.step = int(1e9 / self.freq)
        self.start = pd.Timestamp(start, 'ns').asm8
        self.length = length
        self.shape = (length,)
        self.dtype = np.dtype('datetime64[ns]')

    def __len__(self):
        return self.length

    @methdispatch
    def __getitem__(self, x):
        if x == Ellipsis:
            return self.arange(0, self.length, 1)
        raise IndexError(self._index_error)

    @__getitem__.register(slice)
    def __getslice__(self, x):
        start, stop, step = self.positive(x)
        return self.arange(start, stop, step)

    @__getitem__.register(tuple)
    def __gettuple__(self, x):
        return self.__getitem__(x[0])

    @__getitem__.register(int)
    def __getint__(self, x):
        x = self.positive(x)
        if 0 <= x < self.length:
            return self(x)
        raise IndexError(str(x) + ' is outside of 0-' + str(self.length))

    @__getitem__.register(list)
    def __getlist__(self, x):
        arr = None
        if isinstance(x[0], bool) and self.length == len(x):
            arr = np.where(x)
        elif isinstance(x[0], int):
            arr = np.array(x)
        if arr is not None:
            return self.from_arr(arr)
        raise IndexError(self._index_error)

    @__getitem__.register(np.ndarray)
    def __getnp__(self, x):
        if x.dtype == 'bool' and self.length == len(x):
            arr = np.where(x)
        else:
            arr = x
        if arr is not None:
            return self.from_arr(arr)
        raise IndexError(self._index_error)

    def from_arr(self, arr: np.ndarray) -> np.ndarray:
        """ Return datetime array from index array
        Params:
            arr (np.ndarray): Index array
        Returns:
            np.ndarray[np.datetime64]
        """
        return pd.DatetimeIndex(self.start + (arr * self.step))

    def __call__(self, x: int) -> np.ndarray:
        return self.start + (self.positive(x) * self.step)

    def arange(self, start: int, stop: int, step: int = 1) -> np.ndarray:
        """ Return evenly spaced numpy datetime array from
        start / stop / step index
        Params:
            start (int): Starting index of array
            stop (int): Stop index of array
            step (int): Step between index elements
        Returns:
            np.ndarray[np.datetime64]: Datetime array
        """
        dt_start = self(start)
        dt_stop = self(stop)
        dt_step = self.step * step
        return np.arange(dt_start, dt_stop, dt_step)

    @methdispatch
    def positive(self, x):
        """ Returns array length - x if x is less than 0
        Params:
            x (int): Array integer
        Returns:
            int: positive array integer
        """
        if x < 0:
            return self.length + x
        return x

    @positive.register(np.ndarray)
    def _(self, x):
        x[x < 0] = self.length + x[x < 0]
        if (x < 0).any():
            raise IndexError('Some indx outside of range')
        return x

    @positive.register(slice)
    def _(self, x):
        start = self.positive(x.start if x.start else 0)
        stop = self.positive(x.stop if x.stop else self.length)
        step = x.step if x.step else 1
        if start < 0:
            start = 0
        if stop < 0:
            stop = 0
        return start, stop, step


def file_datehour(fn: str):
    """ Converts the RADAR CSV output filename to a Timestamp
    Params:
        fn (str): The filename
    Returns:
        pd.Timestamp
    """
    return pd.Timestamp(os.path.normpath(fn).split(os.path.sep)[-1][0:13].replace('_', 'T'), tz='UTC')


def create_divisions(files: List[str]) -> List[np.datetime64]:
    """ Create Dask Dataframe datetime divisions from RADAR CSV file names
    Params:
        files (List[str]): List of file names
    Returns:
        List[np.datetime64]: Dask partition divisions
    """
    try:
        divisions = [file_datehour(fn) for fn in files]
        divisions += [file_datehour(files[-1]) + pd.Timedelta(1, 'h')]
    except (IndexError, ValueError):
        divisions = None
    return divisions
