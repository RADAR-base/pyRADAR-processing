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
        self.path = os.path.join(where, name)

        self._schema = kwargs.get('schema')
        self._specification = kwargs.get('specification')
        if self._schema is not None:
            dtypes = self._schema.dtypes
        elif self._specification is not None:
            dtypes = self._schema.dtypes
        else:
            dtypes = None
        self._data = self._make_dask_df(where, name, dtypes=dtypes, **kwargs)

    def __getitem__(self, key):
        return self._data[key].compute()

