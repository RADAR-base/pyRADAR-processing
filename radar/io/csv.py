#!/usr/bin/env python3
import pandas as pd
import dask.dataframe as dd
import glob, os, sys
import csv
from ..defaults import TIME_COLS
from ..util.specifications import ModalitySpec
from ..util.avro import RadarSchema
from .generic import RadarTable


class CsvTable(RadarTable):
    def _make_dask_df(self, where, name, compression=None):
        folder = os.path.join(where, name)
        if compression is None:
            return dd.read_csv(os.path.join(folder, '*.csv'))
        else:
            return dd.read_csv(os.path.join(folder, '*.csv.*'),
                               compression=compression,
                               blocksize=None)

class CsvDataGroup():
    pass


def write_csv(arr, fname: str = '', fieldnames: list = None, **kwargs):
    f = open(fname, 'w') if fname else sys.stdout
    writer = csv.writer(f, **kwargs)
    if fieldnames:
        writer.writerow(fieldnames)
    for row in arr:
        writer.writerow(row)
    if f is not sys.stdout:
        f.close()

def _read_files(files, sort: str = None, index: str = None, **kwargs):
    df = pd.concat(pd.read_csv(f, **kwargs) for f in files)
    if sort:
        df = df.sort_values(by=sort)
    if index:
        df = df.set_index(index)
    return df

def _read_folder(path: str, extension: str = '.csv',
                sort: str = None, **kwargs):
    files = glob.glob(os.path.join(path, '*' + extension))
    if files:
        return _read_files(files, sort, **kwargs)
    print('No files found in', path)
    return None

def read_folder(path: str, schema: RadarSchema = None,
                specification: ModalitySpec = None, **kwargs):

    def schema_spec_kwargs(schema=None, spec=None):
        argdict = {}
        if schema is None:
            return argdict
        cols = schema.get_col_names()
        argdict['dtype'] = {col:dtype for col, dtype in
                            zip(cols, schema.get_col_numpy_types())
                            if col not in TIME_COLS}
        argdict['usecols'] = cols

        time_columns = TIME_COLS
        if spec is not None:
            time_columns = list(set((*TIME_COLS, *spec.time_columns())))
        argdict['parse_dates'] = time_columns
        argdict['date_parser'] = date_parser
        return argdict

    argdict = schema_spec_kwargs(schema, specification)
    if kwargs is not None:
        argdict.update(kwargs)

    return _read_folder(path, **argdict)
