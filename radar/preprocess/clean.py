""" Cleaning and aggregation functions for RADAR data
"""
from functools import wraps
import numpy as np
import pandas as pd
from . import validators


def last(x):
    """ Returns the last element of a series
    Params:
        x (pd.Series)
    Returns:
        scalar of x.dtype
    """
    return x.iloc[-1]


def validate(validator):
    """ Wraps function to run a validator on dataframe input
    """
    def decorator(func):
        @wraps(func)
        def wrap(df, *args, **kwargs):
            df = df[validator(df)]
            return func(df, *args, **kwargs)
        return wrap
    return decorator


@validate(validators.android_phone_contacts)
def android_phone_contacts(df: pd.DataFrame) -> pd.DataFrame:
    """ Returns daily aggregate of valid phone_contacts.
    Sampling frequency of phone_contacts can be high -
    but only really need daily. There are also occasional bugs
    (see validator.android_phone_contacts)
    Params:
        df (pd.DataFrame): phone_contacts dataframe
    Returns:
        pd.DataFrame: daily aggregate of phone_contacts
    """
    df = df[validators.android_phone_contacts(df)]
    return df.groupby(df.index.floor('1D')).agg({
        'projectId': last,
        'userId': last,
        'sourceId': last,
        'timeReceived': last,
        'contactsAdded': sum,
        'contactsRemoved': sum,
        'contacts': lambda x: np.median(x).astype(df['contacts'].dtype)})


@validate(validators.questionnaire_phq8)
def questionnaire_phq8(df: pd.DataFrame) -> pd.DataFrame:
    """ Returns a dataframe with only validated answers
    Params:
        df (pd.DataFrame): melted PHQ8 dataframe
    Returns:
        pd.DataFrame: valid PHQ8 answers
    """
    return df


@validate(validators.questionnaire_rses)
def questionnaire_rses(df: pd.DataFrame) -> pd.DataFrame:
    """ Returns a dataframe with only validated answers
    Params:
        df (pd.DataFrame): melted RSES dataframe
    Returns:
        pd.DataFrame: valid RSES answers
    """
    return df


def connect_fitbit_sleep_stages(df: pd.DataFrame) -> pd.Series:
    """ Cleans fitbit_sleep_stages dataframe
    Maps UNKNOWN -> AWAKE (bug in early connector)
    Params:
        df (pd.DataFrame): sleep stages dataframe
    Returns:
        pd.DataFrame
    """
    df.loc[df['level'] == 'UNKNOWN', 'level'] = 'AWAKE'
    return df


CLEANERS = {
    'android_phone_contacts': android_phone_contacts,
    'connect_fitbit_sleep_stages': connect_fitbit_sleep_stages,
    'questionnaire_phq8': questionnaire_phq8,
    'questionnaire_rses': questionnaire_rses
}
