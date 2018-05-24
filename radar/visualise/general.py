#!/usr/bin/env python3
import matplotlib.pyplot as plt

def group_plot(dataframe, ycols, groupby, xcol=None, **plotkwargs):
    groups = dataframe.groupby(groupby)[ycols]
    f = plt.figure()
    z = 1
    for label, group in groups:
        ax = plt.subplot(len(groups), 1, z)
        group.plot(y=ycols, x=xcol, ax=ax, **plotkwargs)
        z = z + 1
    plt.subplots_adjust(hspace=0)
    return f


def time_view(dataframes, timeframe, ycols, y2cols=None, xcol=None,
              titles=None, fig=None, **plotkwargs):
    """Plots data columns from Pandas dataframes at a certain time interval

    Parameters
    __________
    dataframes : list
        List of pandas dataframes with a timestamp index. (Alternatively, a
        shared x-axis column name supplied by xcol)
    timeframe: tuple
        Two strings containing the start and end date/time of the plot
    ycols: list
        List of list of the dataframe column names. Each element of the list
        corresponds to one subplot. Each element contains a list of  the names of
        columns for the corresponding dataframe.
    y2cols: list
        The same as ycols, but plotted on a second y-axis
    xcol: str
        An optional string specifying the x-axis column of each dataframe if
        the index is not used.
    titles: list
        List of strings for each subplot (dataframes) title.
    """
    z_max = len(dataframes)
    if not fig:
        fig = plt.figure()
    if not y2cols:
        y2cols = [False for i in range(z_max)]
    if not titles:
        titles = [False for i in range(z_max)]

    for z in range(z_max):
        ax = plt.subplot(z_max, 1, z+1)
        dataframes[z].loc[timeframe[0]:timeframe[1]].plot(y=ycols[z], secondary_y=y2cols[z], x=xcol, ax=ax,
                           **plotkwargs)
        if titles[z]:
            ax.set_title(titles[z])
        if z < z_max-1:
            ax.set_xlabel('')
            ax.set_xticks([])
            ax.set_xticklabels([])
    return fig
