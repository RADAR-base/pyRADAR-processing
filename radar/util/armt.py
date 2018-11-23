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

def melt(df):
    """
    Melts a questionnaire CSV into same number of columns
    Needs cleaning up
    """
    df = df.drop_duplicates()
    ids = [c for c in df if 'value.answers' not in c]
    def melt_row(row, ids):
        melt = row.melt(id_vars=ids)
        melt['arrid'] = [(lambda x: x[-2])(x) for x
                in melt['variable'].str.split('.')]
        melt['field'] = [(lambda x: x[-1])(x) for x
                in melt['variable'].str.split('.')]
        col_data = [(x[0], x[1]['value'].values) for x in melt.groupby('field')]
        melt = pd.DataFrame([x[1][ids].iloc[0] for x in melt.groupby('arrid')])
        melt = melt.reset_index(drop=True)
        for col, data in col_data:
            melt[col] = data
        return melt
    out = pd.concat(melt_row(df[i:i+1], ids) for i in range(len(df)))
    out['startTime'] = out.startTime.astype('float')
    out['endTime'] = out.endTime.astype('float')
    return out

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
            The corresponding RADAR definition parsed with the json std library.
        Returns
        _______
        row_dict: dict
            A dict representation of the row from which a dataframe can be made.
        """
        out_row = row.to_dict()
        for i in range(n):
                qid = out_row['value.answers.{}.questionId'.format(i)]
                entry = defintion_entry_from_label(definition, qid)
                code = row['value.answers.{}.value'.format(i)]
                for field, func in fields.items():
                    key = 'value.answers.{}.{}'.format(i,field)
                    out_row[key] = func(entry, code)
        return out_row

    if fields is None:
        fields = {'questionLabel': key_func('field_label'),
                  'valueLabel': code_label}
    n = 1 + max([int(c.split('.')[2]) for c in df if 'answers' == c[6:13]])
    out = [add_columns(row) for row in iterrows(df)]
    return pd.DataFrame(out, columns=arrange_cols(fields, n))

def branch_logic_is_true(row, branch_logic):
    """ Evaluates a RADAR definition question entry's branch logic
    for a given CSV row.

    Parameters
    __________
    row: pandas dataframe row
    branch_logic : str
        The 'evaluated_logic' field of the definition entry.

    Returns
    _________
    bool
        Whether the branch logic evaluates to True or False.
        If branch_logic is an empty string, returns True.
    """
    def question_val(row, question_id):
        idx = next(c for c in row.index if row[c] == question_id).split('.')[2]
        return row['value.answers.{}.value'.format(idx)]

    if branch_logic == '':
        return True
    pattern = "responses\['(.+)'\] ([!=]=) (.+)"
    name, equality, val = re.match(pattern, branch_logic).groups()
    res = (str(question_val(row, name)) == val)

    return {'==': True, '!=': False}[equality] == res

def infer_questionId(df, definition):
    """ Parses through an aRMT dataframe and uses the definition
    to add questionId columns. Not recommended, espeically for ESM, because it
    assumes that there are no missing answers (shouldn't happen, but does)

    Parameters
    __________
    df: pandas.DataFrame
        Dataframe from reading the aRMT csv file.
    definition: list of dicts
        The corresponding RADAR definition parsed with the json std library.
    Returns
    _______
    out: pandas.DataFrame
        Dataframe with additional questionId columns.
    """
    def add_questionId_columns(row, definition, ans_n):
        """ Adds given columns to the row using the definition.
        Parameters
        _________
        row : pandas dataframe row
            A row corresponding to a single questionnaire submission.
        definition : list of dicts
            The corresponding RADAR definition parsed with the json std library.

        Returns
        _______
        row_dict : dict
            A dict representation of the row from which a dataframe can be made.
        """
        i = 0
        out_row = row.to_dict()
        func = key_func('field_name')
        for entry in definition:
            if i >= ans_n:
                log.error('Fewer answers ({}) than expected'.format(ans_n))
                break
            branch_logic = entry['evaluated_logic']
            if branch_logic_is_true(row, branch_logic):
                code = row['value.answers.{}.value'.format(i)]
                key = 'value.answers.{}.questionId'.format(i)
                if np.isnan(out_row.get(key, np.nan)):
                    out_row[key] = func(entry, code)
                i += 1
        return out_row

    ans_n = 1 + max([int(c.split('.')[2]) for c in df.keys()
                     if 'value.answers' in c])
    out = pd.DataFrame((add_questionId_columns(row, definition, ans_n)
                        for row in iterrows(df)))
    return out[arrange_cols(['questionId'], ans_n)]

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
