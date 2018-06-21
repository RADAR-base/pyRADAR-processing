#!/usr/bin/env python3
import os
from ..common import to_datetime

class RadarTable():
    def __init__(self, where, name, **kwargs):
        """ Initialize a generic RadarTable. Used as a parent class for
        actual types of data.
        Parameters
        _________
        where: str
            A path to folder containing the modality file/folder
        name: str
            The name of the table
        schema: RadarSchema (optional)
            A RADAR schema for the data modality.
        specification: RadarSpecification (optional)
            A RADAR specification for the data modality. Can provide time
            columns for the value columns.
        timecols: list of str (optional)
            Convert these columns to datetime64
        infer_time: bool (optional) Default: True
            Infer time columns from column names and convert them to datetime64.
        dtype: dict (optional)
            A dictionary of column datatypes for the table. It is preferentially
            taken from a given schema or specification.
        """
        self.where = where
        self.name = name
        self._schema = kwargs.get('schema')
        self._specification = kwargs.get('specification')
        self._timecols = kwargs.geT('timecols') if 'timecol' in kwargs else []
        self._infer_time = kwargs.get('infer_time')
        dtype = kwargs.get('dtype')
        if self._schema is not None:
            dtype = self._schema.dtype()
        if self._specification is not None:
            self._timecols.extend(self._specification.time_columns())
        self.dtype = dtype
        self._kwargs = kwargs

    def _load_data(self):
        self._data = self._make_dask_df(where=self.where, name=self.name,
                                        dtype=self.dtype, **self._kwargs)

        if self._infer_time is not False:
            self._timecols.extend([col for col in self._data.columns
                                       if 'time' in col])

        self._timecols = list(set(self._timecols))
        if self._timecols:
            self._data[self._timecols] = \
                    to_datetime(self._data[self._timecols])

    def __getitem__(self, key):
        if not hasattr(self, '_data'):
            self._load_data()
        return self._data[key].compute()

