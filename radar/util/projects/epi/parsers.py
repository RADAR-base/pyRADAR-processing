#!/usr/bin/env python3
import pandas as pd
import numpy as np

DAY = pd.Timedelta('1D')

class RedcapCsv():
    """ A class to parse information from an EPI Redcap CSV extract.
        The data dictionary can be found on the radar-base github.
    """
    def __init__(self, csv_path, timezone):
        """
        Parameters
        __________
        csv_path : str
            path of the csv file
        timezone : str / tz representation
            The timezone that the CSV has times recorded in.
        """
        self.df = pd.read_csv(csv_path)
        self.tz = timezone

    def seizures(self):
        """
        Returns
        _______
        labels : dict
            A dict where each key is a patient code with another dict as
            a value. That dict has keys representing labels ('Seizure') and
            values of a dataframe with start and end times.
        """
        df = self.df.copy()
        cols = ('clin_start', 'clin_end', 'eeg_start', 'eeg_end')
        for c in cols:
            df[c] = pd.to_datetime(df.rep_seizure_date + 'T' +
                                   df['rep_seizure_' + c])
        df['start'] = df[['clin_start', 'eeg_start']].min(axis=1)
        df['end'] = df[['clin_end', 'eeg_end']].min(axis=1)
        df.start = df.start.dt.tz_localize(self.tz)
        df.end = df.end.dt.tz_localize(self.tz)
        labels = {}
        for i in range(1, max(self.df.record_id) + 1):
            ptc = df[df.record_id == i]
            code = ptc.patient_code.iloc[0]
            seizures = ptc.iloc[1:]
            labels[code] = {}
            labels[code]['Seizure'] = \
                    seizures[['start', 'end']].reset_index(drop=True)
        return labels

    def info(self):
        """
        Returns
        _______
        info : dict
            A dict where each key is a patient code with another dict as
            a value. That dict has keys corresponding to patient information
            fields - enrolment date, recording start, and recording end
        """
        info = {}
        for i in range(1, max(self.df.record_id) + 1):
            ptc = self.df[self.df.record_id == i]
            code = ptc.patient_code.iloc[0]
            enrolled = ptc.enrolment_date.iloc[0]
            recording_start = ptc.start_date.iloc[0]
            recording_end = ptc.recording_end_date.iloc[0]
            info[code] = {}
            info[code]['enrolment_date'] = \
                    pd.to_datetime(enrolled).tz_localize(self.tz)
            info[code]['recording_start'] = \
                    pd.to_datetime(recording_start).tz_localize(self.tz)
            info[code]['recording_end'] = \
                    pd.to_datetime(recording_end).tz_localize(self.tz) + DAY
        return info

