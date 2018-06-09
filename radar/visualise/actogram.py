#!/usr/bin/env python3
import matplotlib.pyplot as plt

def plot(dataframe, datacol='value.x'):
    groups = dataframe.groupby([dataframe.index.year, dataframe.index.month,
                                dataframe.index.day])
    n = len(groups)
    fig, axes = plt.subplots(len(groups), 1, sharey=True, figsize=(8, n*0.15))
    for i, group in enumerate(groups):
        group[1].plot(y=datacol, ax=axes[i], color='k', kind='area',
                      yticks=[], sharex=True)
        plt.subplots_adjust(hspace=0)
        axes[i].legend_.remove()
    mticks = axes[n-1].get_xaxis().get_majorticklabels()
    mticks[1].set_text('00:00')
    mticks[2].set_text('00:00')
    return fig
