#!/usr/bin/env python3
import glob
import yaml
import requests
from collections import OrderedDict
from packaging import version
from ..defaults import config


class ProjectSpecs(dict):
    """
    A dictionary class to hold all YML specifications relating to a
    RADAR project
    Parameters
    __________
    device_specifications : list of DeviceSpec
        A list of device specification objects
    """
    def __init__(self, device_specifications):
        self.devices = {dev.name: dev for dev in device_specifications}
        for dev in self.devices.values():
            self.update(dev)

    def __repr__(self):
        repr_string = 'Specification dictionary with:\n' + \
                      'Devices: {}\n'.format(', '.join(self.devices)) + \
                      'Specifications: {}'.format(', '.join(self))
        return repr_string


class DeviceSpec(dict):
    """
    A dictionary class to store RADAR YML specifications
    Parameters
    _________
    yml: dict
        A dictionary loaded with yaml.load(file)
    """
    def __init__(self, yml: dict):

        for attr, val in yml.items():
            if not isinstance(val, str):
                continue
            setattr(self, attr, val)

        for attr in ('vendor', 'model', 'version'):
            if not hasattr(self, attr):
                setattr(self, attr, '')

        self.name = '_'.join((self.vendor, self.model))

        super(DeviceSpec, self).__init__([(mdl['topic'], ModalitySpec(mdl))
              for mdl in yml['data'] if ('data' in yml and 'topic' in mdl)])


class ModalitySpec(dict):
    """
    A dictionary class to store the modalities of a RADAR specification.
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
        return -1

    def _type_columns(self, coltype):
        return [name for name, col in self.items()
                if 'type' in col and
                col['type'] == coltype]

    def timecols(self):
        return self._type_columns('TIMESTAMP')

    def timedeltas(self):
        return self._type_columns('DURATION')


class FieldSpec(OrderedDict):
    """
    A class to store fields of modalities in RADAR specifications.
    """
    def __repr__(self):
        repr_string = '"{}" field: (({}))'.format(self['name'],
                '), ('.join([': '.join((k, v)) for k, v in self.items()]))
        return repr_string


def specifications_from_directory(path=config.specifications.dir):
    files = glob.glob(path + '/**/*.yml', recursive=True)
    device_specs = {}
    for fn in files:
        with open(fn) as f:
            spec = DeviceSpec(yaml.load(f, yaml.SafeLoader))
        if (spec.name not in device_specs or
                (version.parse(spec.version) >
                 version.parse(device_specs[spec.name].version))):
            device_specs[spec.name] = spec
    return ProjectSpecs(device_specs.values())


def specifications_from_github(repo_owner, repo_name, sha):
    url = 'https://api.github.com/repos/{}/{}/contents/specifications/'\
        .format(repo_owner, repo_name)

    rawurl = 'https://raw.githubusercontent.com/{}/{}/{}/'\
        .format(repo_owner, repo_name, sha)

    def ls(relpath):
        requrl = url + relpath
        return requests.get(requrl,
                            params={'ref': 'master' if sha is None else sha})

    def dl(relpath):
        return requests.get(rawurl + relpath).content

    folders = ('active', 'connector', 'monitor', 'passive')
    files = [f['path'] for folder in folders for f in ls(folder).json()]
    ymls = [yaml.load(dl(spec), yaml.SafeLoader) for spec in files]
    return ProjectSpecs([DeviceSpec(y) for y in ymls])
