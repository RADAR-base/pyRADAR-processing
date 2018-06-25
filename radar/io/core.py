#!/usr/bin/env python3
import re
from functools import lru_cache
from collections import Counter
from dask.utils import import_required
from dask.bytes.core import _filesystems, get_fs_token_paths
from dask.bytes.utils import infer_compression, infer_storage_options

from .fs import LocalFileSystem
from ..common import log
_filesystems['file'] = LocalFileSystem

def infer_file_format(f):
    compression = infer_compression(f)
    if compression:
        f = f[:-(len(compression)+1)]
    file_split = f.split('.')
    file_format = file_split[-1] if len(file_split) > 1 else None
    return (file_format, compression)

def infer_data_format(f, ftype, include='.*', exclude='.*schema.json'):
    include = re.compile(include)
    exclude = re.compile(exclude)
    if ftype is 'file':
        out = infer_file_format(f)
    else:
        fs = get_fs(**infer_storage_options(f))
        formats, compressions = \
                zip(*(infer_file_format(x) for x in fs.list_files(f)
                      if include.match(x) and not exclude.match(x)))
        if len(set(formats)) > 1:
            log.warning('Not all file formats in {} are the same'.format(f))
        if len(set(compressions)) > 1:
            log.warning('Not all compressions in {} are the same'.format(f))
        out = (Counter(formats).most_common(1)[0][0],
               Counter(compressions).most_common(1)[0][0])
    return out

def f_cond(files, whitelist=None, blacklist=None):
    if whitelist is None:
        whitelist = folders
    if blacklist is None:
        blacklist = []
    return [f for f in files if f in whitelist and f not in blacklist]

def out_paths(path, sep, files, *args, **kwargs):
    files = f_cond(files, *args, **kwargs)
    return [path.rstrip(sep) + sep + f for f in files]

def get_project_contents(path, subprojects=None, participants=None,
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

def get_participant_dir(path, whitelist=None, blacklist=None, **kwargs):
    blacklist = blacklist if blacklist is not None else []
    whitelist = whitelist if whitelist is not None else []
    fs = get_fs(**infer_storage_options(path))
    folders = fs.list_folders(path)
    files = fs.list_files(path)


    return data_paths

def load_data_path(path, **kwargs):
    pass

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
