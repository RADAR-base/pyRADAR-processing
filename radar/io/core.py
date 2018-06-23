#!/usr/bin/env python3
from functools import lru_cache
from dask.bytes.core import _filesystems
from dask.bytes.utils import import_required

from ..common import to_datetime

def determine_protocol(path):
    protocol = path.split('://')
    return protocol[0] if len(protocol) > 1 else 'file'

def load_data_path(path, **kwargs):
    protocol = determine_protocol(path)
    pass

def search_path_for_data(path, *args, **kwargs):
    pass

def open_file():
    pass

@lru_cache(maxsize=2)
def get_fs(protocol, storage_options=None):
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
