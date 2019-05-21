#!/usr/bin/env python3
""" Feather format multi-file IO
Store a dask dataframe in a folder of multiple feather files.
"""
import os
import pyarrow.feather as ft
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from .core import glob_path_for_files
from .generic import create_divisions


def to_feather(df: pd.DataFrame, path: str) -> None:
    """ DataFrame to multi-file feather format. Uses the first row
    of first column as the file name. It is assumed to be a datetime.
    Params:
        df (pd.DataFrame): Dataframe to save
        path (str): Path to folder
    Returns:
        None
    """
    if df.empty:
        return
    if not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=True)
    os.makedirs(path, exist_ok=True)
    ts = df.iloc[0, 0].strftime('%Y%m%d_%H%M')
    out_path = path + '/' + ts + '.feather'
    pd.DataFrame.to_feather(df, out_path)


def to_feather_dask(ddf: dd.DataFrame, path: str, compute: bool = True):
    """ DataFrame to multi-file feather format. Uses the first row
    of first column as the file name. It is assumed to be a datetime.
    Each partition is saved to an individual file.
    Params:
        ddf (dask.dataframe.DataFrame): Dask Dataframe to save
        path (str): Path to folder
    Returns:
        None or delayed object (if compute=False)
    """
    if compute:
        ddf.map_partitions(to_feather, path, meta=object).compute()
    return ddf.map_partitions(to_feather, path, meta=object)


def read_feather_dask(path: str) -> dd.DataFrame:
    """ Read a collection of feather files with datetime name
    into a delayed dataframe
    Params:
        path (str): Path to feather folder
    Returns:
        dask.dataframe.DataFrame
    """
    def get_meta(path):
        table = ft.read_table(path)
        index = table.schema[0].name
        return table.schema.empty_table().to_pandas().set_index(index)

    @delayed
    def read_pandas(path):
        return ft.read_feather(path)

    paths = glob_path_for_files(path, '*feather')
    paths.sort()
    meta = get_meta(paths[0])
    index = meta.index.name
    delayed_objs = [read_pandas(p).set_index(index) for p in paths]
    divisions = create_divisions(paths)
    ddf = dd.from_delayed(delayed_objs, meta=meta, divisions=divisions)
    return ddf
