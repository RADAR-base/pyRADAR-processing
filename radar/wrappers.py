#!/usr/bin/env python3
import os
from collections.abc import MutableMapping, KeysView, ItemsView, ValuesView
import pandas as pd
from fsspec import filesystem
from fsspec.utils import infer_storage_options
from . import config
from .generic import update
from .common import abs_path, log
from .io.loaders import search_project_dir, search_dir_for_data, load_data_path


class RadarObject(MutableMapping):
    _keys_view = KeysView
    _items_view = ItemsView
    _values_view = ValuesView
    def __init__(self, path, populate=False, parent=None, name=None, **kwargs):
        self.parent = parent
        self._subobject_class = RadarObject
        specs = infer_storage_options(path)
        self.path = specs.pop('path')
        self.fs = filesystem(**specs)
        self.path = self.fs.info(self.path)['name']
        self.name = name if name else self.path.split(self.fs.sep)[-1]
        self.store = dict()
        if populate:
            self._populate_store(populate=True)
        self._populated = populate
    def _list_subobjects(self):
        return self.fs.listdir(self.path)
    def _populate_store(self, populate=False):
        paths = self._list_subobjects()
        for p in paths:
            if p['type'] == 'directory':
                basename = p['name'].split(self.fs.sep)[-1]
                self.store[basename] = self._subobject_class(p['name'], parent=self)
        self._populated = True
    def __setitem__(self, key, val):
        self.store[key] = val
    def __getitem__(self, key):
        if not self._populated:
            self._populate_store()
        split_key = key.split('/')
        if len(split_key) == 1:
            return self.store[key]
        else:
            base_key = split_key[0]
            sub_key = '/'.join(split_key[1:])
            return self.store[base_key][sub_key]
    def __delitem__(self, key):
        del self.store[key]
    def __iter__(self):
        return iter(self.store)
    def __len__(self):
        return len(self.store)
    def keys(self):
        if not self._populated:
            self._populate_store()
        return self._keys_view(self)
    def items(self):
        if not self._populated:
            self._populate_store()
        return self._items_view(self)
    def values(self):
        if not self._populated:
            self._populate_store()
        return self._values_view(self)
    def _ipython_key_completions_(self):
        return list(self.keys())
    def _get_attr_or_parents(self, attr):
        if hasattr(self, attr) and getattr(self, attr) is not None:
            res = getattr(self, attr)
        elif getattr(self, 'parent') is None:
            res = None
        else:
            res = self.parent._get_attr_or_parents(attr)
        return res
    def __str__(self):
        return (self.__class__.__module__ + '.' +
                self.__class__.__name__ + ': "' + self.name + '"')
    @property
    def schemas(self):
        return self._get_attr_or_parents('_schemas')
    @property
    def specifications(self):
        return self._get_attr_or_parents('_specifications')
    @property
    def armt_definitions(self):
        return self._get_attr_or_parents('_armt_definitions')
    @property
    def armt_protocols(self):
        return self._get_attr_or_parents('_armt_protocols')


class Project(RadarObject):
    """
    """
    def __init__(self, path, **kwargs):
        """
        Parameters
        __________
        path : list / str
            Path(s) containing project data
        name : str
            The name of the project
        participants : list
            A list of participants to load in paths
        blacklist : list
            Files/folders to ignore in paths
        schemas : None
            --
        specifications : None
            --
        armt_definitions : None
            --
        armt_protocols : None
            --
        """
        super(Project, self).__init__(path, **kwargs)
        self._keys_view = ProjectKeysView
        self._items_view = ProjectItemsView
        self._values_view = ProjectValuesView
        self._subobject_class = Participant
        self._schemas = kwargs.get('schemas')
        self._specifications = kwargs.get('specifications')
        self._armt_definitions = kwargs.get('armt_definitions')
        self._armt_protocols = kwargs.get('armt_protocols')
        self._whitelist = kwargs.get('participants')
        self._blacklist = kwargs.get('blacklist', [])
    def _list_subobjects(self):
        all_paths = self.fs.listdir(self.path)
        whitelist = [p['name'] for p in all_paths] if self._whitelist is None \
            else self._whitelist
        blacklist = self._blacklist
        return [p for p in all_paths if
                p['name'] in whitelist and
                p['name'] not in blacklist]
    @property
    def participants(self):
        return self.keys()

class ProjectKeysView(KeysView):
    def __repr__(self):
        keys_str = ', '.join(self._mapping.participants)
        return 'ProjectParticipantKeys[' + keys_str + ']'

class ProjectItemsView(ItemsView):
    def __repr__(self):
        items_str = ', '.join(['("{name}": {p})'.format(name=name,
                                                        p=self._mapping[name])
                               for name in self._mapping.participants])
        return 'ProjectParticipantItems[' + items_str + ']'


class ProjectValuesView(KeysView):
    def __repr__(self):
        vals_str = ', '.join([str(self._mapping[name])
                              for name in self._mapping.participants])
        return 'ProjectParticipantValues[' + vals_str + ']'


class Participant(RadarObject):
    """ A class to hold data and methods concerning participants/subjects in a
    RADAR trial. Typically intialised by opening a Project.
    """
    def __init__(self, path, **kwargs):
        super(Participant, self).__init__(path, **kwargs)
        self._keys_view = ParticipantKeysView
        self._items_view = ParticipantItemsView
        self._values_view = ParticipantValuesView
        self._subobject_class = RadarData
    @property
    def data(self):
        return self.keys()

class ParticipantKeysView(KeysView):
    def __repr__(self):
        keys_str = ', '.join(self._mapping.data)
        return 'ParticipantDataKeys[' + keys_str + ']'

class ParticipantItemsView(ItemsView):
    def __repr__(self):
        items_str = ', '.join(['("{name}": {p})'.format(name=name,
                                                        p=self._mapping[name])
                               for name in self._mapping.data])
        return 'ParticipantDataItems[' + items_str + ']'


class ParticipantValuesView(KeysView):
    def __repr__(self):
        vals_str = ', '.join([str(self._mapping[name])
                              for name in self._mapping.data])
        return 'ParticipantDataValues[' + vals_str + ']'

class RadarData():
    def __init__(self, *args, **kwargs):
        pass
