#!/usr/bin/env python3
from avro import schema
import pandas as pd
import numpy as np
import glob, os, json
from ..defaults import _SCHEMA_DIR, _SCHEMA_KEY_FILE, _DEVICE

AVRO_NP_TYPES = {
    'null': None,
    'boolean': np.bool_,
    'int': np.int32,
    'long': np.int64,
    'float': np.float32,
    'double': np.float64,
    'bytes': np.bytes_,
    'string': np.object,
    'enum': np.object,
}

class ProjectSchemas(dict):
    def __init__(self, schema_dir=_SCHEMA_DIR, key_schema=_SCHEMA_KEY_FILE):
        if key_schema is not None:
            with open(key_schema, 'r') as f:
                key_schema = f.read()

        avsc_files = glob.glob(schema_dir + '/**/*.avsc', recursive=True)
        for schema_path in avsc_files:
            rel_path = os.path.relpath(schema_path, schema_dir)
            split_rel_path = rel_path.split(os.path.sep)
            if split_rel_path[0] not in ('active', 'passive'):
                continue
            name = ''.join((_DEVICE if split_rel_path[0] == 'passive' else '',
                            split_rel_path[-1]))[:-5]
            with open(schema_path, 'r') as f:
                try:
                    obj = RadarSchema(value_json=f.read(), key_json=key_schema)
                    self[name] = obj
                except schema.SchemaParseException:
                    print(name, 'could not be parsed because it has',
                          'an unresolved nested schema.\n',
                          'Nested schemas are not currently supported.')


class RadarSchema():
    """
    A class for use with RADAR-base key-value pair schemas. Initialise with a
    json string representation of the schema.
    """

    INDEX = 'value.time'
    SORT = 'value.time'
    TIME_COLS = ['value.time', 'value.timeReceived']
    CSV_EXT = '.csv'
    usekeys = True

    class _FakeSchema(object):
        pass

    def __init__(self, schema_json=None, key_json=None, value_json=None):
        """
        This class is initiated with a json string representation of a
        RADAR-base schema.
        Parameters
        __________
        schema_json: string (json)
            A json string representation of a key-value pair RADAR-base schema
        key_json: string (json)
            A json string representation of a key RADAR-base schema
        value_json: string (json)
            A json string representation of a value RADAR-base schema
        __________
        Either schema_json or value_json must be specified. key_json may also
        be given alongside value_json.
        """
        if schema_json:
            self.schema = schema.Parse(schema_json)
        elif value_json:
            if key_json:
                self.schema = schema.Parse(combine_key_value_schemas(key_json,
                                                                     value_json))
            else:
                self.schema = self._FakeSchema()
                self.schema.fields = [self._FakeSchema()]
                self.schema.fields[0].type = schema.Parse(value_json)
                self.schema.fields[0].name = 'value'
        else:
            raise ValueError('Please provide json representation of a'
                             'key-value schema or a value schema with or'
                             'without a seperate key schema.')


    def get_col_info(self, func=lambda x:x, *args):
        """
        Values from schema columns and their parent fields can be retrieved by
        a given function.
        """
        return [func(col, field, *args) for field in self.schema.fields
                                        for col in field.type.fields]

    def get_col_info_by_key(self, *keys):
        """
        Gets values from a schema column by its dict key. Multiple keys can be
        supplied for nested values.
        """
        def get_info_rec(col, par, *keys):
            return col.props[keys[0]] if len(keys) == 1 else \
                   get_info_rec(col.props[keys[0]], par, *keys[1:])
        return self.get_col_info(get_info_rec, *keys)

    def get_col_names(self,):
        """
        Returns an array of column names for the csv created by the schema
        """
        def get_name(col, parent):
            return parent.name + '.' + col.name
        return self.get_col_info(func=get_name)

    def get_col_types(self,):
        """
        Returns an array of strings naming Avro datatypes for each csv column
        created by the schema
        """
        def get_type(col, *args):
            typeval = col.type.type
            if typeval == 'union':
                return [sch.type for sch in col.type.schemas]
            else:
                # Should check for enum/other complex types as well
                return typeval
        return self.get_col_info(func=get_type)

    def get_col_numpy_types(self):
        """
        Returns an array of the equivilent numpy datatypes for the csv file for
        the schema
        """
        def convert_type(data_type):
            # numpy arrays don't want union types.
            if isinstance(data_type, list):
                dtype = [convert_type(x) for x in data_type if x != 'null']
                if len(dtype) == 1:
                    return dtype[0]
                else:
                    return np.object
            else:
                return AVRO_NP_TYPES[data_type]
        return [convert_type(x) for x in self.get_col_types()]


def combine_key_value_schemas(key_schema, value_schema):
    """ Combined a RADAR key schema and a RADAR value schema.
    Needs cleaning
    Parameters
    __________
    key_schema: string (json)
        The json string representation of a RADAR key schema. By default
        observation_key
    value_schema: string (json)
        The json string representation of a RADAR value schema.
    """
    key_dict = json.loads(key_schema)
    value_dict = json.loads(value_schema)
    combined_schema = \
            '{\n' + \
            '  "type" : "record",\n' + \
            '  "name" : "' + value_dict['name'] + '",\n' + \
            '  "namespace" : "' + key_dict['namespace'] + '_' + \
                value_dict['namespace'] + '",\n' + \
            '  "doc" : "combined key-value record",\n' + \
            '  "fields"  : [ {\n' + \
            '      "name" : "key",\n' + \
            '      "type" : ' + key_schema + ',\n' + \
            '      "doc" : "Key of a Kafka SinkRecord"\n' + \
            '    }, {\n' + \
            '      "name" : "value",\n' + \
            '      "type" : ' + value_schema + ',\n' + \
            '      "doc" : "Value of a Kafka SinkRecord"\n' + \
            '  } ]\n' + \
            '}'

    return combined_schema
