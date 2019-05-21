#!/usr/bin/env python3
from typing import List
import pandas as pd
from functools import lru_cache
from dask.bytes.core import _filesystems, get_fs_token_paths
from dask.bytes.utils import infer_storage_options
from .fs import LocalFileSystem, get_fs


def glob_path_for_files(path, file_extension):
    fs = get_fs(**infer_storage_options(path))
    if fs.isfile(path):
        return [path]
    return fs.glob(path + fs.sep + file_extension)


def files_newer_than(files: List[str], newer_than: float):
    fs = get_fs(**infer_storage_options(files[0]))
    return [f for f in files if fs.getctime(f) > newer_than]


def terminal_folders(path):
    fs = get_fs(**infer_storage_options(path))
    paths = []
    for folder in fs.list_folders(path):
        paths.extend(terminal_folders(path + fs.sep + folder))
    if not paths:
        paths.append(path)
    return paths


