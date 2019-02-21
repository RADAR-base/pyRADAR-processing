#!/usr/bin/env python3
import os
import json
import glob
import numpy as np
import pandas as pd
from ..defaults import config, schemas

AVRO_NP_TYPES = {
    'null': 'object',
    'boolean': 'bool',
    'int': 'int32',
    'long': 'int64',
    'float': 'float32',
    'double': 'float64',
    'bytes': 'bytes',
    'string': 'object',
    'enum': 'object',
}

BLANK_KEY = {"namespace": "NoKey", "fields": []}


class RadarSchema():
    """
    A class for use with RADAR-base key-value pair schemas. Initialise with a
    json string representation of the schema.
    """

    def __init__(self, schema_json, key_json=None):
        """
        This class is initiated with a json dict representation of a
        RADAR-base schema.
        Parameters
        __________
        schema_json: dict
            A json dict representation of a key-value pair RADAR-base schema
            May also only represent the value section of the schema.
        key_json: dict (optional)
            A json dict representation of a key RADAR-base schema
        __________
        """
        self.schema = schema_from_value_or_schema(schema_json, key_json)

    def get_col_info(self, func=lambda x: x, *args):
        """
        Values from schema columns and their parent fields can be retrieved by
        a given function.
        """
        return [func(col, field, *args)
                for field in self.schema['fields']
                for col in field['type']['fields']]

    def get_col_info_by_key(self, *keys):
        """
        Gets values from a schema column by its dict key. Multiple keys can be
        supplied for nested values.
        """
        def get_info_rec(col, par, *keys):
            return col[keys[0]] if len(keys) == 1 else \
                   get_info_rec(col.props[keys[0]], par, *keys[1:])
        return self.get_col_info(get_info_rec, *keys)

    def get_col_names(self,):
        """
        Returns an array of column names for the csv created by the schema
        """
        def get_name(col, parent):
            return parent['name'] + '.' + col['name']
        return self.get_col_info(func=get_name)

    def get_col_types(self,):
        """
        Returns an array of strings naming Avro datatypes for each csv column
        created by the schema
        """
        def get_type(col, *args):
            typeval = col['type']
            typeval_type = type(typeval)
            if typeval_type is list:
                typeval = (t for t in typeval if not t == 'null')
                return next(typeval)
            else:
                return typeval
        return self.get_col_info_by_key('type')

    def get_col_py_types(self):
        """
        Returns an array of the equivilent numpy datatypes for the csv file for
        the schema
        """
        def convert_type(dtype):
            nptype = np.object
            if isinstance(dtype, list):
                if 'null' in dtype:
                    dtype.remove('null')
                if len(dtype) == 1:
                    if dtype[0] in ('float', 'double'):
                        nptype = AVRO_NP_TYPES[dtype[0]]
                    elif dtype[0] in ('int', 'long'):
                        nptype = 'Int64'
            elif isinstance(dtype, dict):
                if dtype['type'] == 'enum':
                    nptype = pd.api.types.CategoricalDtype(dtype['symbols'])
                else:
                    nptype = AVRO_NP_TYPES.get(dtype['type'], np.object)
            else:
                nptype = AVRO_NP_TYPES.get(dtype, np.object)
            return nptype
        return [convert_type(x) for x in self.get_col_types()]

    def dtype(self):
        return {k: v for k, v in
                zip(self.get_col_names(), self.get_col_py_types())}

    def timecols(self):
        return [name for name, doc in
                zip(self.get_col_names(), self.get_col_info_by_key('doc'))
                if 'timestamp' in doc]

    def timedeltas(self):
        return [name for name, doc in
                zip(self.get_col_names(), self.get_col_info_by_key('doc'))
                if 'duration' in doc.lower()]


def schema_from_value_or_schema(schema_json, key_json=None):
    if schema_json['doc'] == 'combined key-value record':
        schema = schema_json
    else:
        if key_json is None:
            key_json = BLANK_KEY
        schema = combine_key_value_schemas(key_json, schema_json)
    return schema


def combine_key_value_schemas(key_json, value_json):
    """ Combined a RADAR key schema and a RADAR value schema.
    Needs cleaning
    Parameters
    __________
    key_schema: dict
        The json dict representation of a RADAR key schema. By default
        observation_key
    value_json: dict
        The json dict representation of a RADAR value schema.
    """
    schema_json = {
            'type': 'record',
            'name': value_json['name'],
            'namespace': '{}_{}'.format(key_json['namespace'],
                                        value_json['namespace']),
            'doc': 'combined key-value record',
            'fields': [
                    {'name': 'key',
                     'type': key_json,
                     'doc': 'Key of a Kafka SinkRecord'},
                    {'name': 'value',
                     'type': value_json,
                     'doc': 'Value of a Kafka SinkRecord'},
                ],
            }
    return schema_json


def schemas_from_commons(path, key_path=None):
    schema_paths = glob.glob(path + '/**/*.avsc', recursive=True)
    schemas = []
    names = []
    whitelist = ('passive', 'connector', 'monitor')
    for sp in schema_paths:
        if os.path.relpath(sp, path).split(os.path.sep)[0] not in whitelist:
            continue
        name = os.path.basename(sp).split('.')[0]
        if 'passive' in sp:
            name = config.schema.device + '_' + name
        names.append(name)
        schemas.append(schema_from_file(sp, key_path))
    return {name: scm for name, scm in zip(names, schemas)}


def schema_from_file(path, key_path=None):
    with open(path) as vf:
        scm_json = json.loads(vf.read())
    if key_path:
        with open(key_path) as kf:
            key_json = json.loads(kf.read())
    else:
        key_json = None
    return RadarSchema(scm_json, key_json=key_json)


def schemas_from_git(path, key=None):
    raise NotImplementedError


def schema_from_url(value_url, key_url=None):
    raise NotImplementedError


if config.schema.dir:
    schemas.update(schemas_from_commons(config.schema.dir, config.schema.key))
