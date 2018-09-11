#!/usr/bin/env python3
import matplotlib.pyplot as plt
import pandas as pd

week = pd.Timedelta('7d')
def weekly_plot(dataframe, datacol='value.x', **kwargs):
    groups = dataframe.groupby([dataframe.index.year, # dataframe.index.month,
                                dataframe.index.week])
    n = len(groups)
    fig, axes = plt.subplots(n, 1, sharey=True, figsize=(8, 2*n))
    for i, group in enumerate(groups):
        ax = axes[i] if str(type(axes)) == "<class 'numpy.ndarray'>" else axes
        start = pd.Timestamp(str(group[0][0])) + pd.Timedelta((group[0][1]-1)*7, 'd')
        end = start + week
        group[1].plot(y=datacol, ax=ax, **kwargs)
        ax.set_xlim(start, end)
        if ax.legend_:
            ax.legend_.remove()
    plt.subplots_adjust(hspace=0.5)
    fig.tight_layout()
    return fig

day = pd.Timedelta('1d')
def daily_plot(dataframe, datacol, **kwargs):
    groups = dataframe.groupby([dataframe.index.year, dataframe.index.month,
                                dataframe.index.day])
    n = len(groups)
    fig, axes = plt.subplots(n, 1, sharey=True, figsize=(3, 0.15*n))
    for i, group in enumerate(groups):
        ax = axes[i] if str(type(axes)) == "<class 'numpy.ndarray'>" else axes
        start = pd.Timestamp(group[1].index[0].date())
        end = start + day
        group[1].plot(y=datacol, ax=ax, legend=False, **kwargs)
        ax.set_xlim(start, end)
        ax.set_yticks([])
        ax.set_xticks([])
        ax.set_xlabel('')
        ax.set_ylabel('')
        plt.axis('off')
        if hasattr(ax, 'legend_') and ax.legend_:
            ax.legend_.remove()
    plt.subplots_adjust(hspace=0)
    plt.tick_params(axis='both', which='both', bottom=False, top=False,
            labelbottom=False, right=False, left=False, labelleft=False)
    return fig
