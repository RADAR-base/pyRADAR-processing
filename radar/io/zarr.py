#!/usr/bin/env python3
from typing import List
from functools import singledispatch
import zarr as _zarr
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
from dask import delayed
from numcodecs import MsgPack


def get_group(path, group, overwrite):
    if group is None:
        store = _zarr.DirectoryStore(path)
        group = _zarr.group(store, overwrite=overwrite)
    return group


@singledispatch
def to_zarr(x, path):
    raise NotImplementedError


@to_zarr.register(pd.DataFrame)
def df_to_zarr(df: pd.DataFrame, path: str, group=None,
               overwrite=False, append=False, **kwargs):
    group = get_group(path, group, overwrite)
    group.attrs['type'] = 'dataframe'
    if not isinstance(df.index, pd.RangeIndex):
        index = df.index.name
        df = df.reset_index()
        group.attrs['index'] = index
    for col in df.columns:
        array_to_zarr(df[col].values, name=col, group=group,
                      append=append, **kwargs)


@to_zarr.register(dd.DataFrame)
def dask_to_zarr(ddf: dd.DataFrame, path: str, group=None,
                 overwrite=False, append=True, chunks=(2097152,), **kwargs):
    def get_divisions(index):
        nchunks = group[index].nchunks
        divisions = np.zeros(nchunks + 1, group[index].dtype)
        chunk = group[index].chunks[0]
        divisions[0:nchunks] = group[index][::chunk]
        divisions[-1] = group[index][-1]
        return divisions

    group = get_group(path, group, overwrite)
    for p in ddf.partitions:
        df_to_zarr(p.compute(), group=group, path=path,
                   append=append, chunks=chunks)
    group['.divisions'] = get_divisions(ddf.index.name)


@to_zarr.register(np.ndarray)
def array_to_zarr(arr: np.ndarray, name: str, path: str = None, group=None,
                  overwrite=False, append=False, **kwargs):
    group = get_group(path, group, overwrite)
    if name in group.array_keys():
        if overwrite:
            group[name] = arr
        if append:
            group[name].append(arr)

    else:
        codec = MsgPack() if arr.dtype == 'object' else None
        group.array(name, data=arr, object_codec=codec, **kwargs)


def read_zarr(path: str, columns: List[str] = None) -> dd.DataFrame:
    store = _zarr.DirectoryStore(path)
    group = _zarr.group(store)
    array_keys = list(group.array_keys())
    divisions = None
    if '.divisions' in array_keys:
        array_keys.remove('.divisions')
        divisions = pd.DatetimeIndex(group['.divisions'][:]).to_list()
    index = group.attrs.get('index', None)
    columns = array_keys if columns is None else columns
    if index and index not in columns:
        columns = columns + [index]
    arrs = [da.from_zarr(group[c]) for c in columns]
    ddf = dd.concat([dd.from_dask_array(a, columns=c)
                     for a, c in zip(arrs, columns)], axis=1)
    if index is not None:
        ddf = ddf.set_index(index, sorted=True, divisions=divisions)
    return ddf
