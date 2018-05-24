#!/usr/bin/env python3
import glob
import os
import yaml
from collections import OrderedDict
from ..defaults import _SPECIFICATION_DIR
from ..common import RecursiveDict


class ProjectSpecs(OrderedDict):
    """
    A class to hold all YML specifications relating to a RADAR project
    """
    def __init__(self, spec_dir: str = _SPECIFICATION_DIR):
        """
        A path (str) to a RADAR specification directory should be given to
        initialise the class. It will use the package default specifications if
        no path is given.
        """
        self.devices = {}
        self.add_spec_folder(spec_dir)

    def __repr__(self):
        repr_string = 'Specification dictionary with:\n' + \
                      'Devices: {}\n'.format(', '.join(self.devices)) + \
                      'Specs: {}'.format(', '.join(self))
        return repr_string

    def add_spec_folder(self, path: str):
        specs = glob.glob(os.path.join(*os.path.split(path), '**', '*.yml'))
        for sp in specs:
            dev = DeviceSpec(sp)
            name = '_'.join((dev.vendor if hasattr(dev, 'vendor') else '',
                             dev.model if hasattr(dev, 'model') else ''))
            if name:
                self.devices[name] = dev
            self.update(dev)


class DeviceSpec(OrderedDict):
    """
    A class to store RADAR YML specifications
    """
    def __init__(self, specification_file: str):
        with open(specification_file) as f:
            yml = yaml.load(f.read())

        for attr, val in yml.items():
            if not isinstance(val, str):
                continue
            setattr(self, attr, val)

        for attr in ('vendor', 'model', 'version'):
            if not hasattr(self, attr):
                setattr(self, attr, '')

        super(DeviceSpec, self).__init__([(mdl['topic'], ModalitySpec(mdl))
              for mdl in yml['data'] if ('data' in yml and 'topic' in mdl)])


class ModalitySpec(OrderedDict):
    """
    A class to store the modalities of a RADAR specification.
    """

    def __init__(self, modal:dict):
        for attr, val in modal.items():
            if attr is not 'fields':
                setattr(self, attr, val)
        if 'fields' in modal:
            self.update([('value.' + field['name'], FieldSpec(field))
                         for field in modal['fields']])

    def __repr__(self):
        repr_string = '{} modality of type "{}" '.format(self.topic,
                       self.type if hasattr(self, 'type') else 'UNKNOWN') +\
                      'with fields: ' + ', '.join(self)
        return repr_string

    def group_fields(self, ):

        return 0

    def _type_columns(self, coltype):
        return [name for name, col in self.items()
                if 'type' in col and
                col['type'] == coltype]

    def time_columns(self):
        return self._type_columns('TIMESTAMP')

    def timedelta_columns(self):
        return self._type_columns('DURATION')


class FieldSpec(OrderedDict):
    """
    A class to store fields of modalities in RADAR specifications.
    """
    def __init__(self, field:dict):
        super(FieldSpec, self).__init__(field)

    def __repr__(self):
        repr_string = '"{}" field: (({}))'.format(self['name'],
                '), ('.join([': '.join((k, v)) for k, v in self.items()]))
        return repr_string
