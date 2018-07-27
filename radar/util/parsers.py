#!/usr/bin/env python3
import re
import numpy as np
import pandas as pd
from collections import OrderedDict

_NAT = np.datetime64('NaT')

_EPI_SEIZURE_COLUMNS = OrderedDict((
    ('aeds_dose', object),
    ('body_position___1', int),
    ('body_position___2', int),
    ('body_position___3', int),
    ('clinical_end', object),
    ('clinical_start', object),
    ('date', object),
    ('day', object),
    ('eeg_end', object),
    ('eeg_start', object),
    ('empatica', int),
    ('imec', int),
    ('other', object),
    ('type', object),
    ('vigilance_state___1', int),
    ('vigilance_state___2', int),
))

_EPI_SEIZURE_PATTERNS = [re.compile(r'seizure(\d+)_(' + col_name + ')') for
                         col_name in _EPI_SEIZURE_COLUMNS]

_DATE_REGEX = re.compile(r'\d\d\d\d-\d\d-\d\d')
_TIME_REGEX = re.compile(r'\d\d:\d\d(:\d\d)?')

def _row_to_dict(csv_header, csv_row):
    return OrderedDict((key, value) for key, value in zip(csv_header, csv_row))

def _csv_to_dict_list(csv_file):
    header = csv_file.readline().rstrip().split(',')
    return [_row_to_dict(header, row.rstrip().split(',')) for row in
            csv_file.readlines()]

class RedcapEpilepsyCSV():
    def __init__(self, csv, timezone='UTC', use_id=None):
        if hasattr(csv, 'readline'):
            self.dict_list = _csv_to_dict_list(csv)
        elif isinstance(csv, str):
            with open(csv, 'r') as f:
                self.dict_list = _csv_to_dict_list(f)
        else:
            raise ValueError(('Please provide either a CSV file handle or',
                              'a path to the CSV file'))
        self.participants = self._get_participants(use_id=use_id)

    def _get_participants(self, use_id=None):
        if use_id is None:
            potential_keys = ['subject_id', 'human_readable_id',
                              'patient_code', 'record_id']
        else:
            potential_keys = [use_id]

        for k in potential_keys:
            id_list = [partic[k] for partic in self.dict_list]
            if all(id_list):
                print('Using "{}" as ID'.format(k))
                return OrderedDict((pid, data) for pid, data
                                   in zip(id_list, self.dict_list))

        raise ValueError('No suitable ID column covers all participants')

    def seizures(self, participants=None):
        """ Returns a Dataframe of seizure events for each specified
        participant.
        Parameters
        __________
        participants : list (optional)
            If set, only the specified participants will be returned.
        Returns
        _______
        seizure_events : OrderedDict
            A dictionary with participant IDs as keys and a dataframe table of
            seizure events as the value.
        """
        def single_participant_events(redcap_participant_dict):
            num_seizures = \
                int(redcap_participant_dict['seizure_recorded'].strip() or 0)
            """seizure_table = np.recarray((num_seizures,),
                                        dtype=list(_EPI_SEIZURE_COLUMNS.items()))"""
            seizure_table = pd.DataFrame(index = pd.RangeIndex(num_seizures),
                                         columns = list(_EPI_SEIZURE_COLUMNS))
            for col_name, val in redcap_participant_dict.items():
                if col_name[:7] != 'seizure':
                    continue
                for pattern in _EPI_SEIZURE_PATTERNS:
                    match = pattern.search(col_name)
                    if match:
                        groups = match.groups()
                        if int(groups[0]) > num_seizures:
                            continue
                        if _EPI_SEIZURE_COLUMNS[groups[1]] == int:
                            val = int(val.strip() or 0)
                        seizure_table[groups[1]][int(groups[0])-1] = val
                        continue
            return date_time_formatter(seizure_table.infer_objects())

        def date_time_formatter(participant_events):
            """ Takes a single participants events and formats the start/end
            datetimes
            """
            def clean_string(string, regex):
                """Returns the first regex match to a string"""
                string = str(string)
                match = regex.search(string)
                return match[0] if match else None
            def clean_date(date):
                """Cleans the date string using _DATE_REGEX"""
                return clean_string(date, _DATE_REGEX)
            def clean_time(time):
                """Cleans the time string using _TIME_REGEX"""
                return clean_string(time, _TIME_REGEX)

            times = ['clinical_start', 'clinical_end', 'eeg_start', 'eeg_end']
            events = participant_events
            events['date'].map(clean_date)
            for col in times:
                events[col] = events[col].map(clean_time)
                events[col] = events['date'] + 'T' + events[col]
                events[col] = pd.to_datetime(events[col])
            return events

        if participants is None:
            participants = self.participants.keys()
        elif isinstance(participants, str):
            participants = [participants]
        seizure_events = OrderedDict()

        for partic in participants:
            seizure_events[partic] = \
                single_participant_events(self.participants[partic])

        return seizure_events


def biovotion_sort_upsample(df, time_idx='value.time'):
    df = df.sort_index(ascending=False).sort_values(by=time_idx, kind='mergesort')
    lens = df.groupby('value.time').apply(len).values
    deltas = np.array([i/j for j in lens for i in range(j)])
    df[time_idx] = (df[time_idx].astype(int) +
                    deltas.dot(10**9).astype(int)).astype(np.datetime64)
    return df
