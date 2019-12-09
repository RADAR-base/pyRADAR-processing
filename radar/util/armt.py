#!/usr/bin/env python3
import re
import numpy as np
import pandas as pd
from ..common import log

_DEF_COLS = ['key.projectId', 'key.userId', 'key.sourceId',
             'value.time', 'value.timeCompleted', 'value.name',
             'value.version']


def iterrows(df):
    return (row[1] for row in df.iterrows())


def populate(df, definition, fields=None):
    """ Populates a melted aRMT dataframe with additional columns based
    on the questionId
    Parameters
    __________
    df : DataFrame
        A dataframe containing aRMT data as read from an extracted RADAR csv.
    definition : list of dicts
        An aRMT definition loaded with json std lib.
    fields : dict
        A dict with field names as keys and functions as values. The function
        should have the definition entry as first argument and the code/answer
        value as the second argument.

    Returns
    ______
    DataFrame
        A DataFrame with the additional column fields.

    """

    def add_columns(row):
        """ Adds given columns to the row using the definition.
        Parameters
        _________
        row: pandas dataframe row
            A row corresponding to a single questionnaire submission.
        definition: list of dicts
            The corresponding RADAR definition parsed with the json std library
        Returns
        _______
        row_dict: dict
            A dict representation of the row from which a dataframe can be made
        """
        out_row = row.to_dict()
        for i in range(n):
            qid = out_row['value.answers.{}.questionId'.format(i)]
            entry = defintion_entry_from_label(definition, qid)
            code = row['value.answers.{}.value'.format(i)]
            for field, func in fields.items():
                key = 'value.answers.{}.{}'.format(i, field)
                out_row[key] = func(entry, code)
        return out_row

    if fields is None:
        fields = {'questionLabel': key_func('field_label'),
                  'valueLabel': code_label}
    n = 1 + max([int(c.split('.')[2]) for c in df if 'answers' == c[6:13]])
    out = [add_columns(row) for row in iterrows(df)]
    return pd.DataFrame(out, columns=arrange_cols(fields, n))


def code_label(entry, code):
    choices = entry['select_choices_or_calculations']
    labels = [c['label'] for c in choices if
              c['code'].strip() == str(code).strip()]
    if len(labels) != 1:
        err = ('Can not map answer code "{}"'
               ' to the following possibilities in question "{}":\n'
               '{}').format(code, entry['field_name'], choices)
        log.warning(err)
        return np.nan
    return labels[0]


def key_func(key):
    def use_key(entry, *args, **kwargs):
        return entry[key]
    return use_key


def arrange_cols(fields, n):
    cols = _DEF_COLS.copy()
    for i in range(n):
        cols.extend('value.answers.{}.{}'.format(i, f)
                    for f in sorted(fields))
        cols.extend('value.answers.{}.{}'.format(i, c)
                    for c in ('questionId', 'value', 'startTime', 'endTime')
                    if c not in fields)
    return cols


def defintion_entry_from_label(definition, label):
    for entry in definition:
        if entry['field_name'] == label:
            return entry
    raise ValueError('No such label {} in definition'.format(label))
