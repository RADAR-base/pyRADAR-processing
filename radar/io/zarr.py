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
               overwrite=False, append=False, zpath='/', **kwargs):
    group = get_group(path, group, overwrite)[zpath]
    group.attrs['type'] = 'dataframe'
    if not isinstance(df.index, pd.RangeIndex):
        index = df.index.name
        df = df.reset_index()
        group.attrs['index'] = index
    for col in df.columns:
        attrs = {'dtype': df[col].values.dtype.str}
        if hasattr(df[col], 'dt'):
            tz = df[col].dt.tz
            attrs['tz'] = str(tz) if tz else tz
        array_to_zarr(df[col].values, name=col, group=group,
                      append=append, attrs=attrs, **kwargs)


@to_zarr.register(dd.DataFrame)
def dask_to_zarr(ddf: dd.DataFrame, path: str, group=None,
                 overwrite=False, append=True, chunks=(16777216,),
                 compute=True, zpath='/'):
    def get_divisions(group, index):
        index = index if index is not None else 'index'
        nchunks = group[index].nchunks
        divisions = np.zeros(nchunks + 1, group[index].dtype)
        chunk = group[index].chunks[0]
        divisions[0:nchunks] = group[index][::chunk]
        divisions[-1] = group[index][-1]
        return divisions

    @delayed
    def ddf_to_zarr(group):
        group = get_group(path, group, overwrite)
        for p in ddf.partitions:
            df_to_zarr(p.compute(), group=group, path=path,
                       append=append, chunks=chunks, zpath=zpath)
        group['.divisions'] = get_divisions(group, ddf.index.name)

    delayed_write = ddf_to_zarr(group)

    if compute:
        return delayed_write.compute()
    return delayed_write


@to_zarr.register(np.ndarray)
def array_to_zarr(arr: np.ndarray, name: str, path: str = None, group=None,
                  overwrite=False, append=False, attrs=None, **kwargs):
    group = get_group(path, group, overwrite)
    if name in group.array_keys():
        if overwrite:
            group[name] = arr
        if append:
            group[name].append(arr)
    else:
        codec = MsgPack() if arr.dtype == 'object' else None
        group.array(name, data=arr, object_codec=codec, **kwargs)

    group[name].attrs.update(attrs if attrs else {})


def read_zarr(path: str, columns: List[str] = None) -> dd.DataFrame:
    store = _zarr.DirectoryStore(path)
    group = _zarr.group(store)
    array_keys = list(group.array_keys())
    index = group.attrs.get('index', None)
    divisions = None
    if '.divisions' in array_keys:
        array_keys.remove('.divisions')
        tz = group[index].attrs.get('tz', None)
        divisions = pd.DatetimeIndex(group['.divisions'][:], tz=tz).to_list()
    column_names = array_keys if columns is None else columns
    if index and index not in column_names:
        column_names = column_names + [index]
    cols = []
    for name in column_names:
        arr = da.from_zarr(group[name])
        col = dd.from_dask_array(arr, columns=name)
        if col.dtype == '<M8[ns]':
            tz = group[name].attrs.get('tz', None)
            col = col.dt.tz_localize(tz)
        cols.append(col)
    ddf = dd.concat(cols, axis=1)
    if index is not None:
        ddf = ddf.set_index(index, sorted=True, divisions=divisions)
    ddf.group = group
    return ddf
