#!/usr/bin/env python3
import re
import dask.dataframe as dd

from functools import lru_cache
from collections import Counter
from dask.utils import import_required
from dask.bytes.core import _filesystems, get_fs_token_paths
from dask.bytes.utils import infer_compression, infer_storage_options

from .fs import LocalFileSystem
from ..common import log

_filesystems['file'] = LocalFileSystem

def f_cond(files, whitelist=None, blacklist=None):
    if whitelist is None:
        whitelist = files
    if blacklist is None:
        blacklist = []
    return [f for f in files if f in whitelist and f not in blacklist]

def out_paths(path, sep, files, *args, **kwargs):
    files = f_cond(files, *args, **kwargs)
    return [path.rstrip(sep) + sep + f for f in files]

def get_project_dir(path, subprojects=None, participants=None,
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


def infer_file_format(f):
    compression = infer_compression(f)
    i = 2 if compression else 1
    file_split = f.split('.')
    file_format = file_split[-i] if len(file_split) > i else None
    return (file_format, compression)

def infer_data_format(f, include='.*', exclude='.*schema.json'):
    include = re.compile(include)
    exclude = re.compile(exclude)
    fs = get_fs(**infer_storage_options(f))
    if fs.isfile(f):
        out = infer_file_format(f) + (True,)
    else:
        formats_comps = list(zip(*(infer_file_format(x)
                                   for x in fs.list_files(f)
                                  if include.match(x) and
                                   not exclude.match(x))))
        if len(list(formats_comps)) == 2:
            formats, compressions = formats_comps
        else:
            formats = [None]
            compressions = [None]
        if len(set(formats)) > 1:
            log.warning('Not all file formats in {} are the same'.format(f))
        if len(set(compressions)) > 1:
            log.warning('Not all compressions in {} are the same'.format(f))
        out = (Counter(formats).most_common(1)[0][0],
               Counter(compressions).most_common(1)[0][0],
              False)
    return out

_data_load_funcs = {}

def get_data_func(data_format, compression, isfile):
    format_comp = (data_format, compression)
    if format_comp in _data_load_funcs:
        return _data_load_funcs[format_comp]
    if data_format == 'csv':
        if compression:
            print('compression')
            func = lambda path, *args, **kwargs: \
                    dd.read_csv(path.rstrip('/') + '/' + '*.csv.*',
                                compression=compression, blocksize=None,
                                *args, **kwargs)
        else:
            func = lambda path, *args, **kwargs: \
                    dd.read_csv(path.rstrip('/') + '/' + '*.csv',
                                *args, **kwargs)
    elif data_format == 'tsv':
        if compression:
            func = lambda path, *args, **kwargs: \
                    dd.read_tsv(path.rstrip('/') + '/' + '*.tsv.*',
                                compression=compression, blocksize=None,
                                *args, **kwargs)
        else:
            func = lambda path, *args, **kwargs: \
                    dd.read_tsv(path.rstrip('/') + '/' + '.tsv',
                                *args, **kwargs)
    elif data_format == 'json':
        if compression:
            func = lambda path, *args, **kwargs: \
                    dd.read_json(path.rstrip('/') + '/' + '*.json.*',
                                compression=compression, blocksize=None,
                                *args, **kwargs)
        else:
            func = lambda path, *args, **kwargs: \
                    dd.read_json(path.rstrip('/') + '/' + '*.json',
                                *args, **kwargs)
    elif data_format == 'parquet' or data_format == 'pq':
        func = dd.read_parquet
    elif data_format == 'orc':
        func = dd.read_orc
    else:
        log.error('Unsupported data format "{}" or compression "{}"'\
                  .format(data_format, compression))
        func = lambda *args, **kwargs: None
    _data_load_funcs[format_comp] = func
    return func

def load_data_path(path, **kwargs):
    data_format, compression, isfile = infer_data_format(path)
    func = get_data_func(data_format, compression, isfile)
    return func(path)

def search_dir_for_data(path, whitelist=None, blacklist=None,
                        include=None, exclude=None, **kwargs):
    subdirs = kwargs.pop('subdirs', [])
    blacklist = blacklist + subdirs if blacklist is not None else None
    fs = get_fs(**infer_storage_options(path))
    out_paths = listdir(path, whitelist=whitelist, blacklist=blacklist,
                        include=include, exclude=exclude)
    for sd in subdirs:
        subpath = path.rstrip(fs.sep) + fs.sep + sd
        if fs.isdir(subpath):
            out_paths.extend(listdir(subpath, **kwargs))

    return {p.split(fs.sep)[-1]: p for p in out_paths}



def listdir(path, whitelist=None, blacklist=None, include=None, exclude=None):
    fs = get_fs(**infer_storage_options(path))
    files = fs.list_files(path) + fs.list_folders(path)
    blacklist = blacklist if blacklist is not None else []
    whitelist = whitelist if whitelist is not None else files
    return out_paths(path, fs.sep, files,
                     whitelist=whitelist,
                     blacklist=blacklist)

@lru_cache(maxsize=2)
def get_fs(protocol, **storage_options):
    """ Creates a filesystem object
    Essentially the same as in dask.bytes.core, but we need a few extra
    functions on the filesystem objects.

    Parameters
    __________
    protocol : str
    storage_options : dict (optional)

    Returns
    _________
    fs : FileSystem
    fs_token : str (?)"""

    if protocol is not 'file':
        raise NotImplementedError('Only local filesystems are supported')

    if protocol in _filesystems:
        cls = _filesystems[protocol]

    elif protocol == 's3':
        raise NotImplementedError('s3 protocol is not yet implemented')
        import_required('s3fs',
                        '"s3fs" is required for the s3 protocol\n"'
                        '    pip install s3fs')
        cls = _filesystems[protocol]

    else:
        raise ValueError('Unsupported protocol "{}"'.format(protocol))

    if storage_options is None:
        storage_options = {}

    fs = cls(**storage_options)
    return fs
