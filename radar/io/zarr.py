#!/usr/bin/env python3
from functools import singledispatch
import zarr
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from numcodecs import MsgPack


@singledispatch
def to_zarr(x, path):
    raise NotImplementedError


@to_zarr.register(pd.DataFrame)
def df_to_zarr(df: pd.DataFrame, path: str, overwrite=True, append=False, group=None, **kwargs):
    if group is None:
        store = zarr.DirectoryStore(path)
        group = zarr.group(store, overwrite=overwrite)
    if not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()
    for col in df.columns:
        object_codec = MsgPack() if df[col].dtype == 'object' else None
        if col in group.array_keys():
            if append:
                group[col].append(df[col].values)
        else:
            group.array(col, data=df[col].values, object_codec=object_codec,
                        overwrite=overwrite)


@to_zarr.register(dd.DataFrame)
def dask_to_zarr(ddf: dd.DataFrame, path: str, group=None, **kwargs):
    if group is None:
        store = zarr.DirectoryStore(path)
        group = zarr.group(store, overwrite=False)
    for p in ddf.partitions:
        to_zarr(p.compute(), group=group, path=path, append=True)
