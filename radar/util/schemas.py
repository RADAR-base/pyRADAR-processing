#!/usr/bin/env python3
from typing import Dict
import os
import json
import glob
import numpy as np
import pandas as pd
import avro.schema
from ..defaults import config, schemas

from functools import lru_cache

AVRO_NP_TYPES = {
    'null': 'object',
    'boolean': 'bool',
    'int': 'Int32',
    'long': 'Int64',
    'float': 'float32',
    'double': 'float64',
    'bytes': 'bytes',
    'string': 'object',
    'enum': 'object',
}

DATETIME_FIELDS = {'time', 'timeReceived', 'dateTime'}
TIMEDELTA_FIELDS = {'duration'}

BLANK_KEY = {"namespace": "NoKey", "fields": []}


def _cache(cls):
    cache = {}
    def wrapper(scm):
        if scm.name not in cache:
            cache[scm.name] = cls(scm)
        return cache[scm.name]
    return wrapper


@_cache
class RadarSchema():
    def _get_timedelta_columns(self):
        def get_dtype(doc):
            dtype = 'timedelta64[s]'
            if 'milli' in doc:
                dtype = 'timedelta64[ms]'
            elif 'micro' in doc:
                dtype = 'timedelta64[us]'
            return dtype
        return {name: get_dtype(field.avro.doc.lower())
                for name, field in self.fields.items()
                if 'duration' in name.lower()
                or 'interval' in name.lower()}
    def __init__(self, avro_schema: avro.schema.RecordSchema):
        self.avro = avro_schema
        self.fields = parse_schema_fields(avro_schema)
        field_names = set(self.fields)
        self.timecols = field_names.intersection(DATETIME_FIELDS)
        self.timedeltas = self._get_timedelta_columns()
        self.dtypes = {'value.' + k: v.nptype for k, v in self.fields.items()
                       if v.nptype is not None}


class SchemaField():
    def convert_type(self, dtype):
        if isinstance(dtype, avro.schema.UnionSchema):
            list_dtype = dtype.to_json()
            if 'null' in list_dtype:
                list_dtype.remove('null')
            if len(list_dtype) == 1:
                if list_dtype[0] in ('int', 'long'):
                    return 'Int64'
                elif list_dtype[0] == 'boolean':
                    return 'object'
                else:
                    return AVRO_NP_TYPES.get(list_dtype[0])
        elif isinstance(dtype, avro.schema.EnumSchema):
            return pd.api.types.CategoricalDtype(dtype.symbols)
        return AVRO_NP_TYPES.get(dtype.type)
    def __init__(self, field: avro.schema.Field):
        self.avro = field
        self.nptype = self.convert_type(field.type)


def parse_schema_fields(schema: avro.schema.RecordSchema, namespace=''):
    def get_schema_if_record(field: avro.schema.Field):
        if field.type.type == 'record':
            return field.type
        if field.type.type == 'union':
            for r in field.type.schemas:
                if r.type == 'record':
                    return r
    def get_schema_if_array(field: avro.schema.Field):
        if field.type.type == 'array':
            return field.type
        if field.type.type == 'union':
            for r in field.type.schemas:
                if r.type == 'array':
                    return r
    out: Dict[str, SchemaField] = {}
    for field in schema.fields:
        rec = get_schema_if_record(field)
        arr = get_schema_if_array(field)
        name = namespace + field.name
        if rec:
            out.update(parse_schema_fields(rec, name + '.'))
        elif arr:
            out.update(parse_schema_fields(arr.items, name + '.\d+.'))
        else:
            out[name] = SchemaField(field)
    return out


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

