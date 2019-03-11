import numpy as np
import pandas as pd
import seaborn as sns

HOUR = pd.Timedelta('1h')
CMAP = [(1, 1, 1)]
CMAP.extend(sns.color_palette('Set2', 20))


def ptc_number(ptc):
    return int(ptc.info['record_id'])


def data_completion_hourly(ddf, start, end, enrolment_date=None):
    """ Returns a vector with elements representing whether data is
    present at an hourly resolution.
    Parameters
    __________
    ddf: dask.dataframe
        A delayed dataframe as produced through radar.io.load().
        Must have a DatetimeIndex.
    start: pd.Timestamp / np.datetime64
    end: pd.Timestamp / np.datetime64
    enrolment_date: pd.Timestamp / np.datetime64 (optional)
        Hours before this date are assigned -1.

    Returns
    _______
    v: np.ndarray
        Array where 1 corresponds to present data, 0 to missing data,
        and -1 to prior to enrolment.
    """
    vec = pd.Series(index=pd.date_range(start=start, end=end, freq='1h'),
                    dtype=int)
    idx = pd.DatetimeIndex(ddf.divisions)
    idx = idx[np.logical_and(idx > vec.index[0], idx < vec.index[-1])]
    vec.loc[idx] = 1
    if enrolment_date:
        vec.loc[:enrolment_date] = -1
    return vec.values


def ptc_completion_hourly(ptc, start, end, modalities=None,
                          enrolment_date=None):
    """ Returns a data completion array and a list of modality names.
    Parameters
    __________
    ptc: radar.Participant
    start: pd.Timestamp / np.datetime64
    end: pd.Timestamp / np.datetime64
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
    completion = np.zeros((len(modalities), 1+(end-start)//HOUR), dtype=int)
    for i, dname in enumerate(modalities):
        if dname not in ptc.data:
            print(f'Participant {ptc.name} has no recorded {dname}')
            continue
        print(dname)
        completion[i, :] = data_completion_hourly(
                ptc.data[dname], start, end,
                enrolment_date=enrolment_date)
    return completion, modalities


def ptc_completion_plot(completion, modalities, start,
        ax=None, enrolment_date=None, cmap=CMAP):
    """ Plots the completion array as a heatmap
    Parameters
    __________
    completion: np.ndarray
        Completion 2d array for a participant
    modalities: list of str
        A list of the modalities, used to label the Y-axis
    start: pd.Timedelta / np.datetime64
    ax: matplotlib axes
    enrolment_date: pd.Timedelta / np.datetime64
    cmap: Colormap

    Returns
    _______
    ax: matplotlib axes
    """
    ax = sns.heatmap(completion, cmap=cmap, cbar=False,
                     xticklabels=672, ax=None)
    # Gaps
    N_x = completion.shape[1]
    N_y = completion.shape[0]
    for i in range(N_y-1):
        ax.hlines(i+1, 0, N_x, color='white', linewidth=2)

    # Outline
    ax.vlines(0, 0, N_y, color='black', linewidth=2)
    ax.vlines(N_x, 0, N_y, color='black', linewidth=2)
    ax.hlines(0, 0, N_x, color='black', linewidth=2)
    ax.hlines(N_y, 0, N_x, color='black', linewidth=2)

    # Enrolment
    if enrolment_date:
        enrolment_idx = (enrolment_date - start)//HOUR
        ax.vline(enrolment_idx-1, 0, N_y,
                 color='red', linewidth=0.5)

    # Labels
    ax.set_ylabel('Data topic')
    ax.set_xlabel('Date')
    xticks = ax.get_xticks()
    ax.set_xticklabels([(start + (tm * HOUR)).strftime('%y-%m-%d')
                        for tm in xticks], rotation=0)
    ax.set_yticklabels(modalities, rotation=0)
    return ax

if __name__ == '__main__':
    import os
    from datetime import date

    import matplotlib.pyplot as plt
    import radar
    from radar.util.projects.epi.parsers import RedcapCsv

    rc = RedcapCsv(csv_path='path/to/rc.csv', timezone='Europe/London')
    proj = radar.Project(name='EPI', path='path/to/project',
            info=rc.info(), datakw={'subdirs':['RADAR']})
    # the datakw 'subdirs' is used to find data folders inside subfolders
    # within a participant folder. E.g. RADAR in KCL EPI.
    modalities = [
            'android_empatica_e4_blood_volume_pulse',
            'android_empatica_e4_acceleration',
            'android_empatica_e4_electrodermal_activity',
    ] # Etc
    os.makedirs('reports/figures/completion_{}'.format(date.today()),
                exist_ok=True)
    for ptc in proj.participants:
        start = ptc.info['recording_start']
        end = ptc.info['recording_end'] + 12*HOUR
        fig = plt.figure(figsize=(12,6))
        completion, _ = ptc_completion_hourly(ptc, start, end, modalities)
        ptc_completion_plot(completion, modalities, start)
        plt.title('{}'.format(ptc.name))
        plt.savefig('reports/figures/completion_{}/{}.png'\
                .format(date.today(), ptc.name),
                    dpi=288, bbox_inches='tight')
        plt.close()
