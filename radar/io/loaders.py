#!/usr/bin/env python3
import base64
from collections import Counter
from functools import partial
import numpy as np
import dask.dataframe as dd
from dask.bytes.utils import infer_compression, infer_storage_options
from .core import get_fs
from .radar import read_prmt_csv, armt_read_csv_funcs, schema_read_csv_funcs
from .feather import read_feather_dask
from ..generic import re_compile
from ..common import log, config

COMBI_DATA = {
    'IMEC': (
        'imec_old_acceleration',
        'imec_old_ecg',
        'imec_old_emg',
        'imec_old_gsr'
    ),
    'IMEC H5': (
        'imec_acceleration',
        'imec_ecg',
        'imec_emg',
        'imec_gsr'
    )
}

_data_load_funcs = {}


def out_paths(path, sep, files, *args, **kwargs):
    def f_cond(files, whitelist=None, blacklist=None):
        if whitelist is None:
            whitelist = files
        if blacklist is None:
            blacklist = []
        return [f for f in files if f in whitelist and
                f not in blacklist and f[0] != '.']
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
    # files = fs.list_files(path)

    blacklist = blacklist if blacklist is not None else []
    subprojects = subprojects if subprojects is not None else []

    sp = out_paths(path, fs.sep, folders,
                   whitelist=subprojects if subprojects else [],
                   blacklist=blacklist)
    ptc = out_paths(path, fs.sep, folders,
                    whitelist=participants,
                    blacklist=subprojects + blacklist)

    return {'subprojects': sp,
            'participants': ptc}


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
                    exclude=kwargs.get('exclude', None))
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


def load_data_path(path, **kwargs):
    if config['schema']['from_local_file']:
        from ..util.schemas import schema_from_file
        SCHEMA_REGEX = re_compile(config['schema']['local_file_regex'])
        fs = get_fs(**infer_storage_options(path))
        if fs.isdir(path):
            schema_files = [path + '/' + x for x in fs.list_files(path)
                            if SCHEMA_REGEX.match(x)]
            if schema_files:
                schema = schema_from_file(schema_files[0])
                f = read_prmt_csv(dtype=schema.dtype(),
                                  timecols=schema.timecols(),
                                  timedeltas=schema.timedeltas())
                log.debug('Loaded schema from local file %s', schema_files[0])
                return f(path, **kwargs)

    func = get_data_func(*infer_data_format(path))
    return func(path, **kwargs)


def get_data_func(name, ext, compression, isfile):
    func = None
    ext_comp = (ext, compression)

    if name in _data_load_funcs:
        return _data_load_funcs[name]
    if ext_comp in _data_load_funcs:
        return _data_load_funcs[ext_comp]

    if name == 'IMEC':
        from .imec import imec_old_all
        func = imec_old_all
    elif name == 'IMEC H5':
        from .imec import imec_h5_all
        func = imec_h5_all

    if ext == 'csv':
        func = read_dd_generic(dd.read_csv, isfile, compression, '*.csv*')
    elif ext == 'tsv':
        func = partial(read_dd_generic(dd.read_csv, isfile,
                                       compression, '*.tsv*'),
                       sep='\t')
    elif ext == 'json':
        func = read_dd_generic(dd.read_json, isfile, compression, '*.json*')
    elif ext in ('parquet', 'pq'):
        func = partial(dd.read_parquet, engine='pyarrow')
    elif ext == 'orc':
        func = dd.read_orc
    elif ext == 'feather':
        func = read_feather_dask
    elif ext == 'zdf':
        try:
            from .zarr import read_zarr
            func = read_zarr
        except ImportError:
            log.error('No "zarr" package found - can not load zarr files')

    if func is None:
        log.error('Unsupported data format "%s" or compression "%s"',
                  ext, compression)
        func = load_none
    _data_load_funcs[ext_comp] = func
    return func


def read_dd_generic(func, isfile, compression, glob):
    def load(path, *args, **kwargs):
        p = path if isfile else path + '/' + glob
        return func(p, compression=compression, blocksize=None,
                    *args, **kwargs)
    return load


def load_none(*args, **kwargs):
    return None


if config['schema']['read_csvs']:
    from ..util import schemas as _schemas
    _data_load_funcs.update(schema_read_csv_funcs(_schemas.schemas))

if config['protocol']['url'] or config['protocol']['file']:
    from ..util import protocol as _protocol
    _data_load_funcs.update(armt_read_csv_funcs(_protocol.protocols))


# Fitbit temp
_data_load_funcs['connect_fitbit_intraday_steps'] = read_prmt_csv(
    dtype={
        'value.time': float,
        'value.timeReceived': float,
        'value.timeInterval': int,
        'value.steps': int
    },
    timecols=['value.time', 'value.timeReceived'],
    timedeltas={'value.timeInterval': 'timedelta64[s]'})

_data_load_funcs['connect_fitbit_intraday_heart_rate'] = read_prmt_csv(
    dtype={
        'value.time': float,
        'value.timeReceived': float,
        'value.timeInterval': int,
        'value.heartRate': int
    },
    timecols=['value.time', 'value.timeReceived'],
    timedeltas={'value.timeInterval': 'timedelta64[s]'})

_data_load_funcs['connect_fitbit_sleep_stages'] = read_prmt_csv(
    dtype={
        'value.dateTime': object,
        'value.timeReceived': float,
        'value.duration': int,
        'value.level': object
    },
    timecols=['value.dateTime', 'value.timeReceived'],
    timedeltas={'value.duration': 'timedelta64[s]'},
    index='dateTime')

_data_load_funcs['connect_fitbit_sleep_classic'] = read_prmt_csv(
    dtype={
        'value.dateTime': object,
        'value.timeReceived': float,
        'value.duration': int,
        'value.level': object
    },
    timecols=['value.dateTime', 'value.timeReceived'],
    timedeltas={'value.duration': 'timedelta64[s]'},
    index='dateTime')


_data_load_funcs['connect_fitbit_time_zone'] = read_prmt_csv(
    dtype={
        'value.timeReceived': float,
        'value.timeZoneOffset': int
    },
    timecols=['value.timeReceived'],
    index='timeReceived')


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

    delayed_read = read_prmt_csv(timecols=['value.time', 'value.timeReceived'],
                                 dtype={'value.data': object})
    return read_csv


_data_load_funcs['android_processed_audio'] = read_processed_audio()
