#!/usr/bin/env python3
import matplotlib.pyplot as plt

def plot(dataframe, datacol='value.x'):
    z = 1
    groups = dataframe.groupby([dataframe.index.year, dataframe.index.month,
                                dataframe.index.day])
    for labels, group in groups:
        ax = plt.subplot(len(groups), 1, z)
        group.plot(y=datacol, ax=ax, color='k')
        plt.xticks(visible=False)
        plt.yticks(visible=False)
        plt.subplots_adjust(hspace=0)
        plt.ylim(0, 1)
        ax.legend_.remove()
        z = z + 1
    plt.show()
    return plt
