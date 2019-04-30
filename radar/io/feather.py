""" Feather format multi-file IO
Store a dask dataframe in a folder of multiple feather files.
"""
#!/usr/bin/env python3
import os
from typing import List
import pyarrow as pa
import pyarrow.feather as ft
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from .core import create_divisions, glob_path_for_files


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


def to_feather_dask(ddf: dd.DataFrame, path: str) -> None:
    """ DataFrame to multi-file feather format. Uses the first row
    of first column as the file name. It is assumed to be a datetime.
    Each partition is saved to an individual file.
    Params:
        ddf (dask.dataframe.DataFrame): Dask Dataframe to save
        path (str): Path to folder
    Returns:
        None
    """
    ddf.map_partitions(to_feather, path, meta=object).compute()


def read_feather_dask(path: str) -> dd.DataFrame:
    """ Read a collection of feather files with datetime name
    into a delayed dataframe
    Params:
        paths (List[str]): Path to feather folder or file
    Returns:
        dask.dataframe.DataFrame
    """
    paths = glob_path_for_files(path, '*feather')
    paths.sort()
    to_pandas = delayed(pa.Table.to_pandas)
    dset = ft.FeatherDataset(paths)
    dset.read_table()
    index = dset.schema[0].name
    meta = dset.schema.empty_table().to_pandas().set_index(index)
    delayed_objs = [to_pandas(t).set_index(index) for t in dset._tables]
    divisions = create_divisions(dset.paths)
    ddf = dd.from_delayed(delayed_objs, meta=meta, divisions=divisions)
    return ddf
