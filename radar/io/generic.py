#!/usr/bin/env python3
import re
import numpy as np
import pandas as pd
import dask.dataframe as dd
from functools import lru_cache
from collections import Counter
from dask.bytes.utils import infer_compression, infer_storage_options
from ..common import log
from .core import get_fs

def out_paths(path, sep, files, *args, **kwargs):
    def f_cond(files, whitelist=None, blacklist=None):
        if whitelist is None:
            whitelist = files
        if blacklist is None:
            blacklist = []
        return [f for f in files if f in whitelist and
                                    f not in blacklist and
                                    f[0] != '.']
    files = f_cond(files, *args, **kwargs)
    return [path.rstrip(sep) + sep + f for f in files]

def listdir(path, whitelist=None, blacklist=None, include=None, exclude=None):
    fs = get_fs(**infer_storage_options(path))
    files = fs.list_files(path) + fs.list_folders(path)
    blacklist = blacklist if blacklist is not None else []
    whitelist = whitelist if whitelist is not None else files
    return out_paths(path, fs.sep, files,
                     whitelist=whitelist,
                     blacklist=blacklist)

def search_project_dir(path, subprojects=None, participants=None,
                         blacklist=None):
    fs = get_fs(**infer_storage_options(path))
    folders = fs.list_folders(path)
    # files = fs.list_files(path)

    blacklist = blacklist if blacklist is not None else []
    subprojects = subprojects if subprojects is not None else []

    sp = out_paths(path, fs.sep, folders,
                   whitelist=subprojects if subprojects else [],
                   blacklist = blacklist)
    ptc = out_paths(path, fs.sep, folders,
                    whitelist = participants,
                    blacklist = subprojects + blacklist)

    return {'subprojects': sp,
            'participants': ptc}


# Data searching
@lru_cache(8)
def re_compile(pattern):
    return re.compile(pattern)

def infer_data_format(f, include='.*', exclude='.*schema.*json'):
    def infer_file_format(f):
        comp = infer_compression(f)
        i = 2 if comp else 1
        file_split = f.split('.')
        ext = file_split[-i]
        return [ext, comp]

    def infer_folder_format(path, include=None, exclude='.*schema.*json'):
        fs = get_fs(**infer_storage_options(path))
        folder_split = path.split(fs.sep)[-1].split('.')
        if len(folder_split) > 1:
            return [folder_split[-1], None]

        if include is not None:
            include = re_compile(include)
        if exclude is not None:
            exclude = re_compile(exclude)
        exts_comps = list(zip(*(infer_file_format(x) for x in fs.list_files(path)
                               if (include is None or include.match(x))
                               and (exclude is None or not exclude.match(x)))))
        exts, comps = exts_comps or [[None], [None]]
        ext = Counter(exts).most_common(1)[0][0]
        comp = Counter(comps).most_common(1)[0][0]
        if len(set(exts)) > 1:
            log.warning('Not all file formats in {} are the same'.format(path))
        if len(set(comps)) > 1:
            log.warning('Not all compressions in {} are the same'.format(path))
        return [ext, comp]

    fs = get_fs(**infer_storage_options(f))
    f = f.rstrip(fs.sep)
    name = f.split(fs.sep)[-1]
    isfile = fs.isfile(f)
    if isfile:
        ext, comp = infer_file_format(f)
    else:
        ext, comp = infer_folder_format(f, include, exclude)
    return [name, ext, comp, isfile]


COMBI_DATA = {'IMEC': ('imec_acceleration', 'imec_gsr',
                       'imec_ecg', 'imec_emg',
                       'imec_temperature', 'imec_pie')}

def search_dir_for_data(path, **kwargs):
    subdirs = kwargs.pop('subdirs', [])
    blacklist = kwargs.pop('blacklist', [])
    if isinstance(subdirs, str):
        subdirs = [subdirs]
    blacklist = blacklist + subdirs
    fs = get_fs(**infer_storage_options(path))
    paths = listdir(path, blacklist=blacklist,
                    whitelist=kwargs.get('whitelist', None),
                    include=kwargs.get('include', None),
                    exclude = kwargs.get('exclude', None))
    for sd in subdirs:
        subpath = path.rstrip(fs.sep) + fs.sep + sd
        if fs.isdir(subpath):
            paths.extend(listdir(subpath, **kwargs))
    out = {}
    for p in paths:
        name = p.split(fs.sep)[-1]
        if name in COMBI_DATA:
            for n in COMBI_DATA[name]:
                out[n] = p
        else:
            out[name] = p
    return out

# Data loading
def load_data_path(path, **kwargs):
    func = get_data_func(*infer_data_format(path))
    return func(path, **kwargs)

_data_load_funcs = {}
def get_data_func(name, ext, compression, isfile):
    func = None
    ext_comp = (ext, compression)
    if name in _data_load_funcs:
        return _data_load_funcs[name]
    elif ext_comp in _data_load_funcs:
        return _data_load_funcs[ext_comp]

    if name == 'IMEC':
        from .imec import Imec
        func = Imec

    if ext == 'csv':
        if compression:
            func = lambda path, *args, **kwargs: \
                    dd.read_csv(path + '/*.csv.*',
                                compression=compression, blocksize=None,
                                *args, **kwargs)
        else:
            func = lambda path, *args, **kwargs: \
                    dd.read_csv(path + '/*.csv',
                                *args, **kwargs)
    elif ext == 'tsv':
        if compression:
            func = lambda path, *args, **kwargs: \
                    dd.read_tsv(path + '/*.tsv.*',
                                compression=compression, blocksize=None,
                                *args, **kwargs)
        else:
            func = lambda path, *args, **kwargs: \
                    dd.read_tsv(path + '/*.tsv',
                                *args, **kwargs)
    elif ext == 'json':
        if compression:
            func = lambda path, *args, **kwargs: \
                    dd.read_json(path + '/*.json.*',
                                compression=compression, blocksize=None,
                                *args, **kwargs)
        else:
            func = lambda path, *args, **kwargs: \
                    dd.read_json(path + '/*.json',
                                *args, **kwargs)
    elif ext == 'parquet' or ext == 'pq':
        func = dd.read_parquet
    elif ext == 'orc':
        func = dd.read_orc
    if func is None:
        log.error('Unsupported data format "{}" or compression "{}"'\
                  .format(ext, compression))
        func = lambda *args, **kwargs: None
    _data_load_funcs[ext_comp] = func
    return func


idx_error = ('only integers, slices (`:`), ellipsis (`...`),',
             ' and integer or boolean arrays are valid indices')
class FakeDatetimeArray(object):
    def __init__(self, start, length, freq=None, step=None):
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
        self.start = pd.Timestamp(start, 'ns').asm8.astype('int64')
        self.length = length
        self.shape = (length,)
        self.dtype = np.dtype('datetime64[ns]')

    def __getitem__(self, x):
        if isinstance(x, tuple):
            x = x[0]
        arr = None
        if isinstance(x, slice):
            start = 0 if x.start is None else x.start
            stop = self.length if x.stop is None else x.stop
            step = 1 if x.step is None else x.step
            arr = np.array(range(start, stop, step))
        if isinstance(x, list):
            if isinstance(x[0], bool) and self.length == len(x):
                arr = np.where(x)
            if isinstance(x[0], int):
                arr = np.array(x)
        elif isinstance(x, np.ndarray):
            if x.dtype == 'bool':
                arr = np.where(x)
            else:
                arr = x
        elif isinstance(x, int):
            arr = np.array(x)
        elif x == Ellipsis:
            arr = np.array(range(self.length))
        if arr is None:
            raise IndexError(idx_error)
        arr = arr[arr >= 0]
        arr = arr[arr < self.length]
        return (self.start + (arr * self.step)).astype('datetime64[ns]')

    def __len__(self):
        return self.length
