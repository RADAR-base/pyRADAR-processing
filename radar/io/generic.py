#!/usr/bin/env python3
import os

class ProjectIO():
    pass


class ParticipantIO():
    pass


class ParticipantData(dict):
    def __repr__(self):
        return 'Participant data tables:\n' + ', '.join(list(self.keys()))

    def available(self):
        print(self.__repr__())


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
            A RADAR specification for the data modality.
        timecols: list of str (optional)
            Convert these columns to datetime64
        infer_time: bool (optional) Default: True
            Infer time columns from column names and convert them to datetime64.
        dtype: dict (optional)
            A dictionary of column datatypes for the table. It is preferentially
            taken from a given schema or specification.
        """
        self.path = os.path.join(where, name)

        self._schema = kwargs.get('schema')
        self._specification = kwargs.get('specification')
        self._timecols = kwargs.get('timecols') if 'timecol' in kwargs else []
        if self._schema is not None:
            dtype = self._schema.dtype
        elif self._specification is not None:
            dtype = self._specification.dtype
        else:
            dtype = kwargs.get('dtype')
        self._data = self._make_dask_df(where, name, dtype=dtype, **kwargs)

        if kwargs.get('infer_time') is not False:
            self._timecols.extend([col for col in self._data.columns
                                       if 'time' in col])

        self._timecols = list(set(self._timecols))
        if self._timecols:
            self._data[self._timecols] = (self._data[self._timecols] * 1e9).\
                                                      astype('datetime64[ns]')

    def __getitem__(self, key):
        return self._data[key].compute()

