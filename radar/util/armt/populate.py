import numpy as np
import pandas as pd
from .common import iterrows

DEF_COLS = ['key.projectId', 'key.userId', 'key.sourceId',
            'value.time', 'value.timeCompleted', 'value.name',
            'value.version']

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

def add_columns(row, definition, ans_n, fields):
    """ Adds given columns to the row using the definition.
    Parameters
    _________
    row: pandas dataframe row
        A row corresponding to a single questionnaire submission.
    definition: list of dicts
        The corresponding RADAR definition parsed with the json std library.
    fields: dict
        A dictionary with new column names as keys and a function to retreive
        the required definition value as the value. The function will be given
        the definition entry as the first parameter and the answer value as the
        second.

    Returns
    _______
    row_dict: dict
        A dict representation of the row from which a dataframe can be made.
    """
    i = 0
    out_row = row.to_dict()
    for entry in definition:
        if i >= ans_n:
            raise ValueError('Fewer answers ({}) than expected'.format(ans_n))
        branch_logic = entry['evaluated_logic']
        if branch_logic_is_true(row, branch_logic):
            for field, func in fields.items():
                key = 'value.answers.{}.{}'.format(i, field)
                code = row['value.answers.{}.value'.format(i)]
                out_row[key] = func(entry, code)
            i += 1
    return out_row

def parse_armt_df(df, definition, fields=None):
    """ Parses through an aRMT dataframe and uses the definition
    to add questionId columns.
    Parameters
    __________
    df: pandas.DataFrame
        Dataframe from reading the aRMT csv file.
    definition: dict
        The corresponding RADAR definition parsed with the json std library.
    fields: dict (optional)
        A dictionary with new column names as keys and a function to retreive
        the required definition value as the value. The function will be given
        the definition entry as the first parameter and the answer value as the
        second.
    Returns
    _______
    out: pandas.DataFrame
        Dataframe with additional questionId columns.
    """
    def arrange_cols(fields, ans_n):
        cols = DEF_COLS.copy()
        for i in range(ans_n):
            cols.extend('value.answers.{}.{}'.format(i, f)
                        for f in sorted(fields))
            cols.extend('value.answers.{}.{}'.format(i, c)
                        for c in ('value', 'startTime', 'endTime'))
        return cols

    def code_label(entry, code):
        choices = entry['select_choices_or_calculations']
        labels = [c['label'] for c in choices if
                  c['code'].strip() == str(code).strip()]
        if len(labels) != 1:
            err = ('Can not map answer code "{}"'
                    ' to the following possibilities in question "{}":\n'
                    '{}').format(code, entry['field_name'], choices)
            raise ValueError(err)
        return labels[0]

    def key_func(key):
        def use_key(entry, *args, **kwargs):
            return entry[key]
        return use_key

    if fields is None:
        fields = {'questionId': key_func('field_name'),
                  'questionLabel': key_func('field_label'),
                  'valueLabel': code_label}

    ans_n = 1 + max([int(c.split('.')[2]) for c in df.keys()
                     if 'value.answers' in c])
    out = pd.DataFrame((add_columns(row, definition, ans_n, fields)
                        for row in iterrows(df)))
    return out[arrange_cols(fields, ans_n)]


