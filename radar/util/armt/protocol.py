#!/usr/bin/env python3
import json
import requests
import numpy as np
import pandas as pd

class Object(object):
    pass


class Protocol(object):
    def __init__(self, json_protocol):
        self._json = json_protocol
        self.name = self._json['name']
        self.completition_time = self._json['estimatedCompletionTime']
        self.questionnaire = Object()
        self.questionnaire.__dict__ = self._json['questionnaire']
        self.questionnaire.github_owner, self.questionnaire.github_repo = \
                self.questionnaire.repository.split('.com/')[1].split('/')[0:2]
        self.questionnaire.definitions = {}
        self.questionnaire.github_file = 'questionnaires/{0}/{0}_armt.json'.\
                                          format(self.questionnaire.name)

        prot = self._json['protocol']
        self.repeat_protocol = Object()
        self.repeat_protocol.__dict__ = prot['repeatProtocol']
        self.repeat_questionnaire = Object()
        self.repeat_questionnaire.__dict__ = prot['repeatQuestionnaire']
        self.reminders = Object()
        self.reminders.__dict__ = prot['reminders']


    def _get_definition_commits(self, cache=True):
        if hasattr(self.questionnaire, '_commits'):
            return self.questionnaire._commits
        url = 'https://api.github.com/repos/{}/{}/commits?path={}&per_page=100'\
                .format(self.questionnaire.github_owner,
                        self.questionnaire.github_repo,
                        self.questionnaire.github_file)
        r = requests.get(url)
        r.raise_for_status()
        c_json = json.loads(r.text)
        commits = pd.DataFrame(columns=['date', 'sha'],
                               data=[[c['commit']['author']['date'], c['sha']]
                                      for c in c_json])
        commits['date'] = pd.to_datetime(commits['date'])
        commits.set_index('date', inplace=True)
        if cache:
            self.questionnaire._commits = commits
        return commits


    def get_definition(self, commit='master', cache=True):
        if cache and commit in self.questionnaire.definitions:
            return self.questionnaire.definitions[commit]
        url = 'https://raw.githubusercontent.com/{}/{}/{}/{}'\
                .format(self.questionnaire.github_owner,
                       self.questionnaire.github_repo,
                       commit,
                       self.questionnaire.github_file)
        r = requests.get(url)
        r.raise_for_status()
        definition = json.loads(r.text)

        if cache:
            self.questionnaire.definitions[commit] = definition
        return definition

    def get_definition_before_date(self, date, cache=True):
        commits = self._get_definition_commits(cache=cache)
        idx = commits[commits.index < date].index
        if len(idx) == 0:
            raise ValueError(('There are no definition commits for "{}" in '
                              '"{}" before the date "{}"'.format(
                                  self.questionnaire.github_file,
                                  self.questionnaire.github_repo,
                                  date)))
        sha = commits.loc[idx.max()]['sha']
        return self.get_definition(commit=sha, cache=cache)


    def protocol_timedelta(self):
        return pd.Timedelta('{} {}'.format(self.repeat_protocol.amount,
                                           self.repeat_protocol.unit))

    def questionnaire_timedeltas(self):
        return pd.Series([pd.Timedelta('{} {}'\
                    .format(i, self.repeat_questionnaire.unit))
                    for i in self.repeat_questionnaire.unitsFromZero])

    def expected_questionnaires(self, time_delta, initial=True, offset='0'):
        timedeltas = pd.Series(dtype='timedelta64[ns]')
        time_delta = pd.Timedelta(time_delta)
        offset = pd.Timedelta(offset)
        repeats = np.ceil(time_delta / self.protocol_timedelta()).astype(int)
        for i in range(0 if initial else 1, repeats):
            deltas = i * self.protocol_timedelta() + \
                    self.questionnaire_timedeltas() + \
                    offset
            timedeltas = timedeltas.append(deltas)
        timedeltas.index = range(repeats - (0 if initial else 1))
        return timedeltas[timedeltas < time_delta]

    def questionnaires_between_times(self, start_time, end_time,
                                     initial=True, offset='0'):
        start_time = pd.Timestamp(start_time)
        end_time = pd.Timestamp(end_time)
        time_delta = end_time - start_time
        deltas = self.expected_questionnaires(time_delta=time_delta,
                                              initial=initial, offset=offset)
        times = start_time + deltas
        return times

    def adherence(self, df, start_time, end_time, timecol='index', **kwargs):
        """ Calculates adherence for a given aRMT dataframe
        Parameters
        _________
        df : pd.DataFrame
            A dataframe with a datetime index or column (set by timecol)
            corresponding to questionnaire submission times.
        start_time : pd.Timestamp
            The start of the time frame of interest.
            Anything that pandas can convert to a Timestamp.
        end_time : pd.Timestamp
            The end of the time frame of interest.
        timecol : str (optional)
            The column to take the datetime value from (default: 'index')
        initial : bool (optional)
            Whether the questionnaires start at time-zero or after the first
            repeat period. (default: True)
        offset : pd.Timedelta (optional)
            An offset to apply to the start_time from which point
            questionnaires are delivered. (default: '0')
            Anything that pandas can convert to a Timedelta
        """
        def interval_sum(completed, expected, starti, endi):
            return sum(np.logical_and((completed > expected.iloc[starti]),
                                      (completed < expected.iloc[endi])))
        expected_times = self.questionnaires_between_times(start_time,
                                                           end_time, **kwargs)
        completed_times = getattr(df, timecol)
        if isinstance(completed_times, pd.Index):
            completed_times = completed_times.to_series()
        exp_end = expected_times.append(pd.Series(end_time))
        intervals = [interval_sum(completed_times, exp_end, i, i+1)
                     for i in range(len(expected_times))]
        return pd.DataFrame.from_items((('datetime',expected_times),
                                 ('questionnaires_completed', intervals)))

def project_from_url(url):
    r = requests.get(url)
    r.raise_for_status()
    protocols = json.loads(r.text)
    protocols = {p['name']: Protocol(p) for p in protocols['protocols']}
    return protocols
