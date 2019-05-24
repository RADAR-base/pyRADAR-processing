import os
import matplotlib.pyplot as plt
import pandas as pd
import dask.dataframe as dd

from ..common import log
from ..util.parsers import timestamp_from_string


pd.plotting.register_matplotlib_converters()


def data_plot(ptc, modalities, start, end, freq, ax=None, events=None, event_bounds=None, event_resample=None, outdir=None):
    """ Plots the given participant data as lineplots
    Parameters
    __________
    ptc: Participant
    modalities: list of str
        A list of the modalities to plot
    start: pd.Timedelta / np.datetime64
    end: pd.Timedelta / np.datetime64
    freq: str
        The data is resampled at the specified frequency
    ax: matplotlib axes
    events: list of tuples
        A list of event timestamp and boolean pairs, a detailed plot of each event with boolean=True will be created, see event_range
    event_bounds: list of tuples
        A list of Timedelta pairs, if specified detailed plots of events will be made for each entry in the list, with the respective bounds
    event_resample: str
        If specified, each event detail plot is resampled at the specified frequency, instead of plotting the raw data
    outdir: str
        If specified, the figure will be saved as an image in that directory, instead of shown as a pyplot window
    """
    if modalities is None:
        modalities = sorted(list(ptc.data.keys()))

    fig = plt.figure(figsize=(20,10), num="{} - raw data".format(ptc.name))

    for i, dname in enumerate(modalities):
        if dname not in ptc.data or not isinstance(ptc.data[dname], dd.DataFrame):
            log.debug("Participant %s has no recorded %s", ptc.name, dname)
            continue
        log.info("Plotting %s for participant %s", dname, ptc.name)
        ddf = ptc.data[dname]
        resampled = ddf.resample(freq).mean()
        resampled = resampled.compute()
        ax = fig.add_subplot(len(modalities), 1, i+1, sharex=ax)
        ax.plot(resampled)
        ax.autoscale(enable=True, tight=True)
        ax.set_title("{} - {}".format(ptc.name, dname.split('_', 2)[-1]))

    log.info("Plotting %d events for participant %s", len(events) if events else 0, ptc.name)
    for ev in events:
        e = timestamp_from_string(ev[0])
        if e < start or e > end: continue

        for subax in fig.get_axes(): subax.axvline(e, color='red', linewidth=2, zorder=10)
        for b in event_bounds:
            if ev[1]: data_detail_plot(ptc, e, modalities, b, outdir, event_resample)

    if not outdir: fig.set_tight_layout(True)

    if outdir: fig.savefig('{}{}{}_data.png'.format(outdir, os.path.sep, ptc.name), dpi=288, bbox_inches='tight')
    else: plt.show()
    plt.close(fig)


def data_detail_plot(ptc, ev, modalities, bounds, outdir=None, resample=None):
    """ Plots the given event in detail as lineplots using bounds as the start and end of the plot, relative to ev
    Parameters
    __________
    ptc: Participant
    ev: datetime
        The event timestamp
    modalities: list of str
        A list of the modalities to plot
    bounds: tuple
        A Timedelta pair, specifying the bounds before and after the event to plot
    resample: str
        If specified, the data is resampled at the specified frequency, instead of plotting the raw data
    outdir: str
        If specified, the figure will be saved as an image in that directory, instead of shown as a pyplot window
    """
    ev_fig = plt.figure(figsize=(20,10), num="{} - event at {} - {} - {}-{}".format(ptc.name, ev, resample if resample else 'raw', int(bounds[0].seconds/60), int(bounds[1].seconds/60)))
    ev_pre = ev - bounds[0]
    ev_post = ev + bounds[1]
    ev_ax=None

    for i, dname in enumerate(modalities):
        if dname not in ptc.data or not isinstance(ptc.data[dname], dd.DataFrame): continue
        ddf = ptc.data[dname]
        if resample: ev_data = ddf.resample(resample).mean().loc[ev_pre:ev_post].dropna().compute()
        else: ev_data = ddf.loc[ev_pre:ev_post] \
            .drop('projectId',axis=1,errors='ignore') \
            .drop('userId',axis=1,errors='ignore') \
            .drop('sourceId',axis=1,errors='ignore') \
            .drop('timeReceived',axis=1,errors='ignore') \
            .dropna().compute()
        ev_ax = ev_fig.add_subplot(len(modalities), 1, i+1, sharex=ev_ax)
        ev_ax.plot(ev_data)
        ev_ax.axvline(ev, color='red', linewidth=2, zorder=10)
        ev_ax.autoscale(enable=True, tight=True)
        ev_ax.set_title("{} - {}".format(ptc.name, dname.split('_', 2)[-1]))

    if not outdir: ev_fig.set_tight_layout(True)

    if outdir: ev_fig.savefig('{}{}{}_event_{}_{}_{}-{}.png'.format(outdir, os.path.sep, ptc.name, ev.strftime('%Y%m%d-%H%M%S'), resample if resample else 'raw', int(bounds[0].seconds/60), int(bounds[1].seconds/60)), dpi=288, bbox_inches='tight')
    else: plt.show()
    plt.close(ev_fig)
