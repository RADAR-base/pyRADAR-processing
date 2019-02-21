""" Validator functions for RADAR data dataframes """
import pandas as pd


def android_phone_contacts(df: pd.DataFrame) -> pd.Series:
    """ Are rows in a phone_contacts dataframe valid
    There are occasions where there are a large number of contacts listed
    (100,000+), which are clearly erroneous. Additionally, sometimes all
    contacts are apparently removed and then readded near instantaneously.
    This function returns false for rows with these problems.
    Params:
        df (pd.DataFrame): Dataframe with 'contacts', 'contactsAdded',
            and 'contactsRemoved' columns.
    Returns:
        pd.Series[bool]
    """
    valid = (
        (df['contacts'] < 32000) &
        (df['contactsAdded'] < (1+df['contacts']//2)) &
        (df['contactsRemoved'] < (1+df['contacts']//2))
    )
    return valid


def questionnaire_phq8(df: pd.DataFrame) -> pd.Series:
    """ Validates PHQ8 responses
    Ensures there are 8 answers with values 0 <= v <= 3
    Params:
        df (pd.DataFrame): Melted PHQ8 dataframe
            (questionnaire dataframes are melted on load by default)
    Returns:
        pd.Series[bool]
    """
    valid = (df['value'].astype(int) <= 3) & (df['value'].astype(int) >= 0)
    valid.loc[df.groupby(df.index).apply(len) != 8] = False
    return valid


def questionnaire_rses(df: pd.DataFrame) -> pd.Series:
    """ Validates RSES responses
    Ensures there are 10 answers with values 0 <= v <= 3
    Params:
        df (pd.DataFrame): Melted RSES dataframe
            (questionnaire dataframes are melted on load by default)
    Returns:
        pd.Series[bool]

    """
    valid = (df['value'].astype(int) <= 3) & (df['value'].astype(int) >= 0)
    valid.loc[df.groupby(df.index).apply(len) != 10] = False
    return valid


VALIDATORS = {
    'android_phone_contacts': android_phone_contacts,
    'questionnaire_phq8': questionnaire_phq8,
    'questionnaire_rses': questionnaire_rses
}
