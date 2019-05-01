#!/usr/bin/env python3
from typing import List
import pandas as pd
from functools import lru_cache
from dask.utils import import_required
from dask.bytes.core import _filesystems, get_fs_token_paths
from dask.bytes.utils import infer_storage_options
from .fs import LocalFileSystem

_filesystems['file'] = LocalFileSystem

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


def glob_path_for_files(path, file_extension):
    fs = get_fs(**infer_storage_options(path))
    if fs.isfile(path):
        return [path]
    return fs.glob(path + fs.sep + file_extension)


def files_newer_than(files: List[str], newer_than: float):
    fs = get_fs(**infer_storage_options(files[0]))
    return [f for f in files if fs.getctime(f) > newer_than]


def file_datehour(fn):
    """ Converts the RADAR CSV output filename to a Timestamp
    Params:
        fn (str): The filename
    Returns:
        pd.Timestamp
    """
    return pd.Timestamp(fn.split('/')[-1][0:13].replace('_', 'T'),
                        tz='UTC')


def create_divisions(files):
    try:
        divisions = [file_datehour(fn) for fn in files]
        divisions += [file_datehour(files[-1]) + pd.Timedelta(1, 'h')]
    except (IndexError, ValueError):
        divisions = None
    return divisions



