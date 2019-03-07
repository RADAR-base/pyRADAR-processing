import pandas as pd
import seaborn as sns
from datetime import datetime
import matplotlib.patches as patches

from ..common import log
from ..util.completion import completion_idx_has_data

def completion_plot(completion, modalities, start, end, freq,
    ax=None, cmap=None, x_tick_mult=24, x_tick_fmt="%y-%m-%d %H:%M",
    enrolment_date=None, events=None, event_radius=0):
    """ Plots the completion array as a heatmap
    Parameters
    __________
    completion: np.ndarray
        Completion 2d array for a participant
    modalities: list of str
        A list of the modalities, used to label the Y-axis
    start: pd.Timedelta / np.datetime64
    end: pd.Timedelta / np.datetime64
    freq: str
        The timedelta string associated with the given completion array
    ax: matplotlib axes
    cmap: Colormap
    x_tick_mult: int
        Multiplier for x ticks, will draw a tick every x_tick_mult hours.
    x_tick_fmt: str
    enrolment_date: pd.Timedelta / np.datetime64
    events: list of tuples of strings
        A list of event timestamp and label pairs, both as strings
        Draws a vertical line at each event, colored as:
            green: data from all modalities is present at the event
            orange: data from at least one modality is present at the event
            red: no data is present at the event
        If event_radius is specified the color checks include the whole event circumference.
    event_radius: float
        Radius around each event in multiples of freq

    Returns
    _______
    ax: matplotlib axes
    """
    td = pd.Timedelta(freq)
    ax = sns.heatmap(completion, cmap=cmap, cbar=False, xticklabels=int(x_tick_mult*(pd.Timedelta('1h')/td)), ax=None)
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

    # Events
    if events:
        for e_stamp_str,e_label in events:
            try:
                e_stamp_unix = float(e_stamp_str)
                e_stamp = pd.Timestamp(e_stamp_unix, unit='s').tz_localize('UTC')
            except:
                # TODO: review usage of tz_localize()
                e_stamp = pd.Timestamp(datetime.strptime(e_stamp_str, '%Y-%m-%d %H:%M:%S')).tz_localize('CET').tz_convert('UTC')

            if e_stamp < start or e_stamp > end: continue

            log.debug("Event at {}: {}".format(e_stamp, e_label))
            e_idx = (e_stamp - start)//td

            e_slice = None
            if event_radius:
                e_start = max(0, (e_stamp - start - (td * event_radius))//td)
                e_end = min(N_x-1, (e_stamp - start + (td * event_radius))//td)
                e_slice = slice(e_start, e_end+1)
                rect = patches.Rectangle((e_start, 0), e_end - e_start, N_y, linewidth=0.5, edgecolor='k', alpha=0.25, zorder=9)
                ax.add_patch(rect)

            has_all = completion_idx_has_data(completion, e_slice if e_slice else e_idx, requirement_function=all)
            has_any = completion_idx_has_data(completion, e_slice if e_slice else e_idx, requirement_function=any)

            if has_all: ax.vlines(e_idx, 0, N_y, color='green', linewidth=2, zorder=10)
            elif has_any: ax.vlines(e_idx, 0, N_y, color='orange', linewidth=2, zorder=10)
            else: ax.vlines(e_idx, 0, N_y, color='red', linewidth=2, zorder=10)

    # Enrolment
    if enrolment_date:
        enrolment_idx = (enrolment_date - start)//td
        ax.vline(enrolment_idx-1, 0, N_y, color='red', linewidth=0.5)

    # Labels
    ax.set_ylabel('Data topic')
    ax.set_xlabel('Date')
    xticks = ax.get_xticks()
    ax.set_xticklabels([(start + (tm * td)).strftime(x_tick_fmt) for tm in xticks], rotation=0)
    ax.set_yticklabels([ m.split('_', 2)[-1] for m in modalities ], rotation=0)

    return ax
