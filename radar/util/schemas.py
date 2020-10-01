#!/usr/bin/env python3
from typing import Dict
import os
import json
import glob
import numpy as np
import pandas as pd
import avro.schema
import fsspec
from ..defaults import config, schemas
import collections
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
DATETIME_FIELDS = {'time', 'timeReceived', 'dateTime', 'timeCompleted',
                    'timeNotification',
                   'answers.\d+.startTime', 'answers.\d+.endTime'}
TIMEDELTA_FIELDS = {'duration'}
_NAMES = avro.schema.Names()


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

    def __init__(self, fullname, name, namespace, fields):
        self.fullname = fullname
        self.name = name
        self.namespace = namespace
        self.fields = fields
        field_names = set(self.fields)
        self.timecols = [drop_arrid(name) for name in
                         field_names.intersection(DATETIME_FIELDS)]
        self.timedeltas = self._get_timedelta_columns()
        self.dtypes = {'value.' + k: v.nptype for k, v in self.fields.items()
                       if v.nptype is not None}

    def __eq__(self, other):
        if isinstance(other, RadarSchema):
            return (self.fields == other.fields and
                    self.fullname == other.fullname)
        return False

    def from_avro(avro_schema: avro.schema.RecordSchema):
        fullname = avro_schema.fullname
        name = avro_schema.name
        namespace = avro_schema.namespace
        fields = parse_schema_fields(avro_schema)
        return RadarSchema(fullname=fullname, name=name,
                           namespace=namespace, fields=fields)


def drop_arrid(name):
    spl = [s for s in name.split('.') if s != '\d+']
    return '.'.join(spl)

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
                elif isinstance(dtype.schemas[1], avro.schema.ArraySchema):
                    return AVRO_NP_TYPES.get(dtype.schemas[1].items.type)
                elif isinstance(dtype.schemas[1], avro.schema.EnumSchema):
                    symbols = dtype.schemas[1].symbols
                    return pd.api.types.CategoricalDtype(symbols)
                else:
                    return AVRO_NP_TYPES.get(list_dtype[0])
            else:
                return None
        elif isinstance(dtype, avro.schema.EnumSchema):
            return pd.api.types.CategoricalDtype(dtype.symbols)
        elif isinstance(dtype, avro.schema.ArraySchema):
            return AVRO_NP_TYPES.get(dtype.items.type)
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
            if isinstance(arr.items, avro.schema.RecordSchema):
                out.update(parse_schema_fields(arr.items, name + '.\d+.'))
            else:
                out[name + '.\d+'] = SchemaField(field)
        else:
            out[name] = SchemaField(field)
    return out


def schemas_from_commons(path, names=_NAMES):
    out: Dict[str, RadarSchema] = {}
    excp_schemas = []
    spec = fsspec.utils.infer_storage_options(path)
    path = spec.pop('path')
    fs = fsspec.filesystem(**spec)
    schema_paths = fs.glob(path + fs.sep + '**.avsc')
    for path in schema_paths:
        try:
            scm = schema_from_file(path, names=names)
            if scm is not None:
                out[scm.fullname] = scm
        except avro.schema.SchemaParseException as exc:
            excp_schemas.append(path)
    for path in excp_schemas:
        scm = schema_from_file(path, names=names)
        if scm is not None:
            out[scm.fullname] = scm
    return out


def schema_from_file(f, names=_NAMES):
    with open(f) as of:
        data = json.load(of)
    try:
        avro_scm = avro.schema.SchemaFromJSONData(data, names=names)
    except avro.schema.SchemaParseException as exc:
        del names.names[data['namespace'] + '.' + data['name']]
        raise exc
    if isinstance(avro_scm, avro.schema.RecordSchema):
        return RadarSchema.from_avro(avro_scm)
