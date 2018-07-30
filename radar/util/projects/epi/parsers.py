#!/usr/bin/env python3
import pandas as pd
import numpy as np

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

    def seizure_times(self):
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
            ptc_code = ptc.patient_code.iloc[0]
            seizures = ptc.iloc[1:]
            labels[ptc_code] = {}
            labels[ptc_code]['Seizure'] = \
                    seizures[['start', 'end']].reset_index(drop=True)
        return labels

    def ptc_info(self):
        pass
