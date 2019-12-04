#!/usr/bin/env python3
import base64
from collections import Counter
from functools import partial
import numpy as np
import dask.dataframe as dd
from dask.bytes.utils import infer_compression, infer_storage_options
from .core import get_fs
from .radar import PrmtCsvReader, armt_read_csv_funcs, schema_read_csv_funcs
from .feather import read_feather_dask
from ..generic import re_compile
from ..common import log, config

_data_load_funcs = {}

def infer_data_format(f, include='.*', exclude='.*schema.*json'):
    def infer_file_format(f):
        comp = infer_compression(f)
        i = 2 if comp else 1
        file_split = f.split('.')
        ext = file_split[-i]
        return [ext, comp]

    def infer_folder_format(path, include=None, exclude='.*schema.*json'):
        folder_split = path.split(fs.sep)[-1].split('.')
        if len(folder_split) > 1:
            return [folder_split[-1], None]
        if include is not None:
            include = re_compile(include)
        if exclude is not None:
            exclude = re_compile(exclude)
        exts_comps = list(zip(*(
            infer_file_format(x) for x in fs.list_files(path)
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


def read_processed_audio():
    def convert_data(x):
        return np.array(base64.b64decode(''.join(x.split('\\n')))
                        .split(b'\n')[1].split(b';')[1:], dtype=float)

    def convert_data_delayed(series):
        return series.map(convert_data)

    def read_csv(path, *args, **kwargs):
        df = delayed_read(path, *args, **kwargs)
        df = df.dropna()
        df['data'] = df['data'].map_partitions(
             convert_data_delayed, meta=('data', object))
        return df

    delayed_read = PrmtCsvReader(timecols=['value.time', 'value.timeReceived'],
                                 dtype={'value.data': object})
    return read_csv
