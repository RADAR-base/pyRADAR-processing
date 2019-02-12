#!/usr/bin/env python3
import json
import requests
import numpy as np
import pandas as pd
from functools import lru_cache
from ..defaults import config, protocols

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


    @lru_cache(maxsize=2)
    def _get_definition_commits(self):
        """ A function to retrieve git commits from the Protocol object repo
        Returns
        _______
        commits : pd.DataFrame
            Dataframe with a 'date' and 'sha' column for each git commit
        """
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
        return commits

    @lru_cache(maxsize=64)
    def get_definition(self, commit='master'):
        """ Retrieves the aRMT definition corresponding to a particular commit
        Parameters
        _________
        commit : str
            sha of the commit. (default: 'master')

        Returns
        ________
        definition : list of dicts
            Definition parsed by json.loads.
            Normally this should be a list containing dicts for each question.

        """
        url = 'https://raw.githubusercontent.com/{}/{}/{}/{}'\
                .format(self.questionnaire.github_owner,
                       self.questionnaire.github_repo,
                       commit,
                       self.questionnaire.github_file)
        r = requests.get(url)
        r.raise_for_status()
        definition = json.loads(r.text)
        return definition

    def get_definition_before_date(self, date):
        """ Retrieves a defintion directly prior to a given date
        Parameters
        __________
        date : str or datetime/Timestamp object
            Use this date to get the definition in use at that time.

        Returns
        ________
        definition : list of dicts
            Definition parsed by json.loads.
            Normally this should be a list containing dicts for each question.
        """
        commits = self._get_definition_commits()
        idx = commits[commits.index < date].index
        if len(idx) == 0:
            raise ValueError(('There are no definition commits for "{}" in '
                              '"{}" before the date "{}"'.format(
                                  self.questionnaire.github_file,
                                  self.questionnaire.github_repo,
                                  date)))
        sha = commits.loc[idx.max()]['sha']
        return self.get_definition(commit=sha)

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
        return pd.TimedeltaIndex(timedeltas[timedeltas < time_delta])

    def questionnaires_between_times(self, start_time, end_time,
                                     enrolment_time=None,
                                     initial=True, offset='0'):
        start_time = pd.Timestamp(start_time)
        end_time = pd.Timestamp(end_time)
        if enrolment_time:
            enrolment_time = pd.Timestamp(enrolment_time)
        else:
            enrolment_time = start_time
        time_delta = end_time - enrolment_time
        deltas = self.expected_questionnaires(time_delta=time_delta,
                                              initial=initial, offset=offset)
        times = enrolment_time + deltas
        return times[times >= start_time + pd.Timedelta(offset)]

    def adherence(self, df, start_time, end_time, **kwargs):
        """ Calculates adherence for a given aRMT dataframe
        Parameters
        _________
        df : pd.DataFrame
            A dataframe with a datetime index
            corresponding to questionnaire submission times.
        start_time : pd.Timestamp
            The start of the time frame of interest.
            Anything that pandas can convert to a Timestamp.
        end_time : pd.Timestamp
            The end of the time frame of interest.
        enrolment_time : pd.Timestamp or None (default: None)
            The time of enrolment, if different from start_time
        initial : bool (optional)
            Whether the questionnaires start at time-zero or after the first
            repeat period. (default: True)
        offset : pd.Timedelta (optional)
            An offset to apply to the start_time from which point
            questionnaires are delivered. (default: '0')
            Anything that pandas can convert to a Timedelta
        """
        def interval_sum(completed, expected, starti, endi):
            return sum(np.logical_and((completed > expected[starti]),
                                      (completed < expected[endi])))

        expected_times = self.questionnaires_between_times(
                start_time, end_time, **kwargs)
        completed_times = df.index
        exp_end = expected_times.append(pd.DatetimeIndex([end_time]))
        intervals = [interval_sum(completed_times, exp_end, i, i+1)
                     for i in range(len(expected_times))]
        return pd.DataFrame(data={'time': expected_times.tz_convert(None).values,
                                  'questions_completed': intervals})

def from_url(url):
    r = requests.get(url)
    r.raise_for_status()
    protocols = json.loads(r.text)
    protocols = {p['name']: Protocol(p) for p in protocols['protocols']}
    return protocols

def from_path(path):
    with open(path, 'r') as f:
        protocols_file = json.load(f)
    protocols = {p['name']: Protocol(p) for p in protocols['protocols']}
    return protocols

if config.protocol.path:
    protocols.update(from_path(config.protocol.path))
elif config.protocol.url:
    protocols.update(from_url(config.protocol.url))

