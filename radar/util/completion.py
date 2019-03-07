import numpy as np
import pandas as pd
import dask.dataframe as dd

from ..common import log

def data_completion(ddf, start, end, freq='1h', enrolment_date=None):
    """ Returns a vector with elements representing whether data is
    present at resolution [freq].
    Parameters
    __________
    ddf: dask.dataframe
        A delayed dataframe as produced through radar.io.load().
        Must have a DatetimeIndex.
    start: pd.Timestamp / np.datetime64
    end: pd.Timestamp / np.datetime64
    freq: str
        Timedelta string determining the frequency (i.e. resolution) of the completion check.
        Anything other than the default '1h' resamples the data which may take significantly more time.
    enrolment_date: pd.Timestamp / np.datetime64 (optional)
        Hours before this date are assigned -1.

    Returns
    _______
    v: np.ndarray
        Array where 1 corresponds to present data, 0 to missing data,
        and -1 to prior to enrolment.
    """
    vec = pd.Series(index=pd.date_range(start=start, end=end, freq=freq), dtype=int)
    if vec.empty or ddf is None : return []

    if freq == '1h':
        idx = pd.DatetimeIndex(ddf.divisions)
        idx = idx[np.logical_and(idx > vec.index[0], idx < vec.index[-1])]
        vec.loc[idx] = 1
    else:
        resampled = ddf.resample(freq).first().dropna()
        resampled = resampled.compute()
        for i,v in vec.iteritems():
            if i in resampled.index: vec.loc[i] = 1

    if enrolment_date:
        vec.loc[:enrolment_date] = -1
    return vec.values




def ptc_completion(ptc, start, end, freq='1h', modalities=None, enrolment_date=None):
    """ Returns a data completion array and a list of modality names.
    Parameters
    __________
    ptc: radar.Participant
    start: pd.Timestamp / np.datetime64
    end: pd.Timestamp / np.datetime64
    freq: str
        Timedelta string determining the frequency (i.e. resolution) of the completion check.
        Anything other than the default '1h' resamples the data which may take significantly more time.
    modalities: list of str
    enrolment_date: pd.Timestamp / np.datetime64

    Returns
    _______
    completion: np.ndarray
        First index corresponds to the modality given in the modalities
        list, the second index corresponds to hourly intervals between
        start and end.
    modalities: list of str
        A list of modalities, the same as input if given.
    """
    if modalities is None:
        modalities = sorted(list(ptc.data.keys()))
    completion = np.zeros((len(modalities), 1+(end-start)//pd.Timedelta(freq)), dtype=int)
    for i, dname in enumerate(modalities):
        if dname not in ptc.data or not isinstance(ptc.data[dname], dd.DataFrame):# or len(ptc.data[dname]) == 0:
            log.debug("Participant %s has no recorded %s", ptc.name, dname)
            continue
        log.debug("Processing %s for participant %s", dname, ptc.name)
        completion[i, :] = data_completion(ptc.data[dname], start, end, freq, enrolment_date=enrolment_date)
    return completion, modalities




def completion_idx_has_data(completion, idx, modalities=None, filter=None, requirement_function=any):
    """
    Parameters
    __________
    completion: np.ndarray
        Completion 2d array for a participant
    idx: int or slice
        Either the direct index of the data where to check, or an index slice of a subset of the data to check.
        If a slice is given, all the indices described by the slice need to have data in the completion.
    modalities: list of str
        The list of modalities associated with the completion array. Only used in combination with filter.
    filter: str
        A simple string filter for modalities.
        If both this and modalities are given, only those modalities are checked that include the filter as a substring.
    requirement_function: func
        The function that poses the requirement for a positive result, with respect to the modality-dimension.

    Returns
    _______
    b: boolean
        True if the requirement is fulfilled, False otherwise.
    """
    # TODO: Can this be any function that takes an iterable and returns a boolean? Can that be checked?
    if requirement_function != any and requirement_function != all:
        log.critical("completion_idx_has_data(): argument 'requirement_function' must be any() or all(). Default: any().")
        raise NotImplementedError("Currently only one of the built-ins any() and all() are allowed as requirement_function")

    idx_data = []
    if not isinstance(idx, slice): idx = slice(int(idx), int(idx)+1)
    if modalities is None or filter is None:
        idx_data = [ all(d[idx]) for d in completion ]
    else:
        mod_idx = [ i for i,v in enumerate(modalities) if filter in v ]
        idx_data = [ all(d[idx]) for i,d in enumerate(completion) if i in mod_idx ]

    return requirement_function(idx_data)
