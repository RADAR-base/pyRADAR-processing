#!/usr/bin/env python3
import re
import dask.dataframe as dd
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
        return [f for f in files if f in whitelist and f not in blacklist]
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

def infer_data_format(f, include='.*', exclude='.*schema.json'):
    def infer_file_format(f):
        comp = infer_compression(f)
        i = 2 if comp else 1
        file_split = f.split('.')
        ext = file_split[-i]
        return [ext, comp]

    def infer_folder_format(path, include=None, exclude='.*schema.json'):
        fs = get_fs(**infer_storage_options(path))
        folder_split = path.split(fs.sep)[-1].split('.')
        if len(folder_split) > 1:
            return [folder_split[-1], None]

        if include is not None:
            include = re.compile(include)
        if exclude is not None:
            exclude = re.compile(exclude)
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
    name = f.split(fs.sep)[-1]
    isfile = fs.isfile(f)
    if isfile:
        ext, comp = infer_file_format(f)
    else:
        ext, comp = infer_folder_format(f, include, exclude)
    return [name, ext, comp, isfile]

def search_dir_for_data(path, whitelist=None, blacklist=None,
                        include=None, exclude=None, **kwargs):
    subdirs = kwargs.pop('subdirs', [])
    blacklist = blacklist + subdirs if blacklist is not None else None
    fs = get_fs(**infer_storage_options(path))
    paths = listdir(path, whitelist=whitelist, blacklist=blacklist,
                    include=include, exclude=exclude)
    for sd in subdirs:
        subpath = path.rstrip(fs.sep) + fs.sep + sd
        if fs.isdir(subpath):
            paths.extend(listdir(subpath, **kwargs))
    return {p.split(fs.sep)[-1]: p for p in paths}


# Data loading
def load_data_path(path, **kwargs):
    func = get_data_func(*infer_data_format(path))
    return func(path, **kwargs)

_data_load_funcs = {}
def get_data_func(name, ext, compression, isfile):
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
    else:
        log.error('Unsupported data format "{}" or compression "{}"'\
                  .format(ext, compression))
        func = lambda *args, **kwargs: None
    _data_load_funcs[ext_comp] = func
    return func

