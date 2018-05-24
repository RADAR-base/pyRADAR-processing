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


# Below are to be deleted/replaced
def uklfr_subject_ext_csv(csv):
    """ A parser to extract seizure event times from the UKLFR _ext.csv files
    Parameters
    __________
    csv: StringIO
        A file handle to the CSV file.

    Returns
    _______
    events: np.array
        A 4 column numpy datetime64 array containing merged labels. Each row
        corresponds to one seizure event.
        The columns are:
            - clinician start time
            - clinician end time
            - EEG start time
            - EEG end time
    labels: dict
        A dictionary with keys:
        ['clin_start', 'clin_end', 'eeg_start', 'eeg_end']
        Each entry is a 1D numpy datetime64 array with the time of the label
        entry.
    """
    clin_start = re.compile('klin.ab', re.IGNORECASE)
    clin_end = re.compile('klin.ae', re.IGNORECASE)
    eeg_start = re.compile('eeg.ab', re.IGNORECASE)
    eeg_end = re.compile('eeg.ae', re.IGNORECASE)
    def determine_events(labels):
        def nearest_end(start, ends, cutoff=600):
            cutoff = np.timedelta64(cutoff, 's')
            try:
                nearest_end = ends[(ends > start).argmax()]
            except ValueError:
                return _NAT
            if not nearest_end:
                return +NAT
            elif nearest_end - start < cutoff:
                return nearest_end
            else:
                return _NAT

        def merge_events(clin_events, eeg_events, cutoff=120):
            events = []
            used_eeg = []
            cutoff = np.timedelta64(cutoff, 's')
            for i, c_ev in enumerate(clin_events):
                events.append([c_ev[0], c_ev[1], _NAT, _NAT])
                start_diffs = abs(eeg_events[:, 0] - c_ev[0])
                if any(start_diffs < cutoff):
                    eeg_idx = start_diffs.argmin()
                    used_eeg.append(eeg_idx)
                    events[i][2] = eeg_events[eeg_idx, 0]
                    events[i][3] = eeg_events[eeg_idx, 1]
            for i, e_ev in enumerate(eeg_events):
                if i in used_eeg:
                    continue
                events.append([_NAT, _NAT, e_ev[0], e_ev[1]])

            return np.array(events, dtype='datetime64')

        clin_events = np.zeros((len(labels['clin_start']), 2),
                                dtype='datetime64[s]')
        for i in range(len(labels['clin_start'])):
            clin_events[i, 0] = labels['clin_start'][i]
            clin_events[i, 1] = nearest_end(start=labels['clin_start'][i],
                                            ends=labels['clin_end'])

        eeg_events = np.zeros((len(labels['eeg_start']), 2),
                              dtype='datetime64[s]')
        for i in range(len(labels['eeg_start'])):
            eeg_events[i, 0] = labels['eeg_start'][i]
            eeg_events[i, 1] = nearest_end(start=labels['eeg_start'][i],
                                           ends=labels['eeg_end'])

        return merge_events(clin_events, eeg_events)

    contents = [line.rstrip().split('|') for line in csv.readlines()]
    labels = {
        'clin_start': [],
        'clin_end': [],
        'eeg_start': [],
        'eeg_end': [],
    }
    for line in contents[1:]:
        time = int(line[0])
        if clin_start.findall(line[5]):
            labels['clin_start'].append(time)
        if clin_end.findall(line[5]):
            labels['clin_end'].append(time)
        if eeg_start.findall(line[5]):
            labels['eeg_start'].append(time)
        if eeg_end.findall(line[5]):
            labels['eeg_end'].append(time)

    for k in labels:
        labels[k] = np.array(labels[k], dtype='datetime64[s]')
    events = determine_events(labels)
    return (events, labels)


def find_kcl_events(annotations_csv):
    """ A parser to extract seizure event times from the UKLFR _ext.csv files
    Parameters
    __________
    csv: StringIO
        A file handle to the CSV file.

    Returns
    _______
    events: np.array
        A 4 column numpy datetime64 array containing merged labels. Each row
        corresponds to one seizure event.
        The columns are: 
            - clinician start time
            - clinician end time
            - EEG start time
            - EEG end time
    labels: dict
        A dictionary with keys:
        ['clin_start', 'clin_end', 'eeg_start', 'eeg_end']
        Each entry is a 1D numpy datetime64 array with the time of the label
        entry.
    """
    csv = annotations_csv
    clin_start = re.compile('clinical.start', re.IGNORECASE)
    clin_end = re.compile('clinical.end', re.IGNORECASE)
    eeg_start = re.compile('seizure.start', re.IGNORECASE)
    eeg_end = re.compile('seizure.end', re.IGNORECASE)
    def determine_events(labels):
        def nearest_end(start, ends, cutoff=600):
            cutoff = np.timedelta64(cutoff, 's')
            try:
                nearest_end = ends[(ends > start).argmax()]
            except ValueError:
                return _NAT
            if not nearest_end:
                return _NAT
            elif nearest_end - start < cutoff:
                return nearest_end
            else:
                return _NAT

        def merge_events(clin_events, eeg_events, cutoff=120):
            events = []
            used_eeg = []
            cutoff = np.timedelta64(cutoff, 's')
            for i, c_ev in enumerate(clin_events):
                events.append([c_ev[0], c_ev[1], _NAT, _NAT])
                start_diffs = abs(eeg_events[:, 0] - c_ev[0])
                if any(start_diffs < cutoff):
                    eeg_idx = start_diffs.argmin()
                    used_eeg.append(eeg_idx)
                    events[i][2] = eeg_events[eeg_idx, 0]
                    events[i][3] = eeg_events[eeg_idx, 1]
            for i, e_ev in enumerate(eeg_events):
                if i in used_eeg:
                    continue
                events.append([_NAT, _NAT, e_ev[0], e_ev[1]])

            return np.array(events, dtype='datetime64')

        clin_events = np.zeros((len(labels['clin_start']), 2),
                                dtype='datetime64[s]')
        for i in range(len(labels['clin_start'])):
            clin_events[i, 0] = labels['clin_start'][i]
            clin_events[i, 1] = nearest_end(start=labels['clin_start'][i],
                                            ends=labels['clin_end'])

        eeg_events = np.zeros((len(labels['eeg_start']), 2),
                              dtype='datetime64[s]')
        for i in range(len(labels['eeg_start'])):
            eeg_events[i, 0] = labels['eeg_start'][i]
            eeg_events[i, 1] = nearest_end(start=labels['eeg_start'][i],
                                           ends=labels['eeg_end'])

        return merge_events(clin_events, eeg_events)

    contents = [line.rstrip().split(',') for line in csv.readlines()]
    labels = {
        'clin_start': [],
        'clin_end': [],
        'eeg_start': [],
        'eeg_end': [],
    }
    for line in contents[1:]:
        time = int(float(line[0]))
        if clin_start.findall(line[3]):
            labels['clin_start'].append(time)
        if clin_end.findall(line[3]):
            labels['clin_end'].append(time)
        if eeg_start.findall(line[3]):
            labels['eeg_start'].append(time)
        if eeg_end.findall(line[3]):
            labels['eeg_end'].append(time)

    for k in labels:
        labels[k] = np.array(labels[k], dtype='datetime64[s]')
    events = determine_events(labels)
    return (events, labels)
