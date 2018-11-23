#!/usr/bin/env python3
import pandas as pd
import numpy as np
from ....generic import update

INFO = (('study',
            ['study',
              'enrolment_date',
              'visit',
              'month']),
        ('study_devices',
            ['fitbit_serial',
             'phone_status',
             'phone_brand',
             'phone_model',
             'old_mobile_device',
             'phone_imei_number',
             'android_version',
             'contract_provided',
             'contract_start',
             'armt_installed',
             'armt_version',
             'prmt_installed',
             'thincit_installed',
             'mindstrong_installed']),
        ('socio_demographics',
            ['dob',
             'age',
             'gender',
             'gender_at_birth',
             'marital_status',
             'living_together',
             'children',
             'no_of_children',
             'children_live_with_you',
             'youngest_child_age',
             'country',
             'eduction_finish_age', # [sic]
             'drop_school_1',
             'qualifications',
             'degree_1',
             'benefits',
             'income_gbp',
             'income_contribution',
             'accommodation',
             'accommodation_satisfaction']))

class RedcapMDD():
    """
    A class to parse information from an EPI Redcap CSV extract.
    The data dictionary can be found on the radar-base github.
    """
    def __init__(self, csv_path, timezone=None,
                 id_cols=None, labels_as_dict=True):
        """
        Parameters
        _________
        csv_path : str
            path of the RedCap csv extract file
        timezone : str / tz representation
            The timezone that was used to record datetimes
        id_cols : list
            A list of id columns
        labels_as_dict : bool
            Return labels as a dict with [subject_id][label_name] = df
            Defaults to True. If False, returns a full dataframe with every ptc
        """
        df = pd.read_csv(csv_path)
        self.id_cols = ['subject_id', 'record_id']
        for c in ['subject_id', 'human_readable_id']:
            df[c].fillna(method='ffill', inplace=True)
        for c in df.columns:
            if 'timestamp' in c:
                df[c] = pd.to_datetime(df[c]).dt.tz_localize(timezone)
        df['enrolment_date'] = pd.to_datetime(df['enrolment_date'])
        df['enrolment_date'] = df['enrolment_date'].dt.tz_localize(timezone)
        self.df = df
        self.tz = timezone
        self.labels_as_dict = labels_as_dict

    def _subset(self, columns):
        id_cols = ['subject_id', 'record_id']
        out = self.df[columns].copy()
        out = out.dropna(how='all')
        for c in id_cols:
            out.insert(0, c, self.df[c].copy())
        return out

    def _to_labels(name):
        def decorator(func):
            def wrapper(*args, **kwargs):
                out = func(*args, **kwargs)
                if not args[0].labels_as_dict:
                    return out
                labels = {}
                for sid, df in out.groupby('subject_id'):
                    labels[sid] = {}
                    labels[sid][name] = df.copy().reset_index(drop=True)
                return labels
            return wrapper
        return decorator

    def ids_sr(self):
        cols = [c for c in self.df.columns if
                c[:4] == 'ids_' and c[4].isnumeric()]
        cols.extend([c for c in self.df.columns if
                     'ids_sr' in c])
        return self._subset(cols)

    @_to_labels('IDS_SR')
    def ids_sr_score(self, *args, **kwargs):
        ids_sr = self.ids_sr(*args, **kwargs)
        ids_sr['ids_sr_score'] = _ids_sr_score(ids_sr)
        ids_sr['ids_sr_severity'] = ids_sr['ids_sr_score'].map(_ids_sr_severity)
        return ids_sr

    def all_labels(self):
        label_func_names = ['ids_sr_score']
        labels = [self.__getattribute__(l)() for l in label_func_names]
        if isinstance(labels[0], dict):
            return self._all_labels_dict(labels)
        else:
            return self._all_labels_df(labels)

    @staticmethod
    def _all_labels_dict(labels):
        out = labels.pop()
        for l in labels:
            update(out, l)
        return out

    @staticmethod
    def _all_labels_df(labels):
        return pd.concat(labels)

    def info(self):
        info = {}
        df = self.df.drop_duplicates('record_id', keep='first')
        for sid, r in df.groupby('subject_id'):
            info[sid] = {}
            info[sid]['record_id'] = r.iloc[0]['record_id']
            for cat, fields in INFO:
                info[sid][cat] = r.iloc[0][fields].to_dict()
        return info


def _ids_sr_score(df):
    """ Scores a IDS-SR dataframe with column ids as in redcap
    Parameters
    __________
    df : pd.DataFrame

    Returns
    _______
    score : np.ndarray
    """
    scoring_cols = [c for c in df.columns if
                    c[:4] == 'ids_' and c[4:].isnumeric()]
    max_cols = (('ids_11', 'ids_12'),
                ('ids_13', 'ids_14'))
    sum_cols = [c for c in scoring_cols if c not in
                ['ids_11', 'ids_12', 'ids_13', 'ids_14']]
    score = np.zeros(len(df))
    for cols in max_cols:
        score += df.loc[:, cols].max(axis=1).fillna(0)
    score += df[sum_cols].sum(axis=1)
    return score

def _ids_sr_severity(score):
    """Returns a depression severity from a IDS_SR score.
    Scale taken from: http://www.ids-qids.org/interpretation.html
    Parameters
    __________
    score : numeric
        IDS-SR total score
    Returns
    ______
    severity : float
        Severity according to above scale. Float to allow NaN if score is NaN.
    """
    if score >= 49:
        severity = 4.0
    elif score >= 39:
        severity = 3.0
    elif score >= 26:
        severity = 2.0
    elif score >= 14:
        severity = 1.0
    elif np.isnan(score):
        severity = score
    else:
        severity = 0.0
    return severity
