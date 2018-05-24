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
        self._data = self._make_dask_df(where, name, **kwargs)
        self._schema = kwargs.get('schema')
        self._specification = kwargs.get('specification')
        if self._schema is not None:
            self._apply_schema(self.schema)
        elif self._specification is not None:
            self._apply_specification(self.specification)

    def __getitem__(self, key):
        return self._data[key].compute()

    def _apply_schema(self, schema):
        pass

    def _apply_specification(self, spec):
        pass

