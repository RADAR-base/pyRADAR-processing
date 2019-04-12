#!/usr/bin/env python3
from functools import lru_cache
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
    def __init__(self, csv_path, timezone=None):
        """
        Params:
            csv_path (str): path of the RedCap csv extract file
            timezone (str): The timezone that was used to record datetimes
        """
        df = pd.read_csv(csv_path, sep=None, engine='python')
        self._id_cols = ['subject_id', 'record_id']
        for c in ['subject_id', 'human_readable_id']:
            df[c].fillna(method='ffill', inplace=True)
        for c in df.columns:
            if 'timestamp' in c:
                df[c] = (pd.to_datetime(df[c], errors='coerce')
                         .dt.tz_localize(timezone))
        df['enrolment_date'] = pd.to_datetime(df['enrolment_date'])
        df['enrolment_date'] = df['enrolment_date'].dt.tz_localize(timezone)
        self.df = df
        self.tz = timezone

    def _subset(self, columns):
        id_cols = ['subject_id', 'record_id']
        out = self.df[columns].copy()
        out = out.dropna(how='all')
        for c in id_cols:
            out.insert(0, c, self.df[c].copy())
        return out

    @property
    @lru_cache(1)
    def ids_sr(self):
        cols = [c for c in self.df.columns if
                c[:4] == 'ids_' and c[4].isnumeric()]
        cols.extend([c for c in self.df.columns if
                     'ids_sr' in c])
        return self._subset(cols)

    @property
    @lru_cache(1)
    def info(self):
        info = {}
        df = self.df.drop_duplicates('record_id', keep='first')
        for sid, r in df.groupby('subject_id'):
            info[sid] = {}
            info[sid]['record_id'] = r.iloc[0]['record_id']
            for cat, fields in INFO:
                info[sid][cat] = r.iloc[0][fields].to_dict()
        return info


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
