#!/usr/bin/env python3
""" A module for the plotting of RADAR data in interactive Bokeh plots

"""
import bokeh.plotting
import bokeh.io
import bokeh.palettes
import bokeh.models
# import pandas as pd

DEFAULT_COLORS = bokeh.palettes.Set1[8]

def time_span(dataframe, ycols, timespan, xcol=None, fig=None, colors=None):
    """ Returns a bokeh line plot of the given columns in a pandas dataframe.
    """
    if fig is None:
        fig = bokeh.plotting.figure(plot_width=800,
                                    plot_height=250,
                                    x_axis_type='datetime',
                                    tools='xpan,xwheel_zoom,box_zoom,reset,save',
                                    active_scroll='xwheel_zoom',
                                    active_drag='xpan')
    df = dataframe.loc[timespan[0]:timespan[1]]
    if xcol is None:
        xcol = df.index
    else:
        xcol = df[xcol]

    if colors is None:
        colors = DEFAULT_COLORS

    n = len(ycols)
    for i, column in enumerate(ycols):
        color = colors[i%len(colors)]
        name = column.split('.')[-1]
        fig.line(xcol, df[column], line_color=color, legend=name if n > 1 else None)
    fig.toolbar.logo = None

    return fig

def add_events(fig, events):
    for ev_name, ev_loc in events:
        if str(ev_loc[0]) == 'NaT':
            continue
        xloc = ev_loc[0].timestamp()*1e3
        yloc = ev_loc[1]
        span = bokeh.models.Span(location=xloc, dimension='height',
                                 line_width=2, line_dash='dashed')
        label = bokeh.models.Label(x=xloc+600, y=yloc, y_units='screen',
                                   text=ev_name)
        fig.add_layout(span)
        fig.add_layout(label)


def multiple_timeseries(xcols, ycols, annotations=None, figargs=None):
    raise NotImplementedError

def save_fig(fig, filename, filetype='html', **kwargs):
    if fig.title.text:
        title = fig.title.text
    else:
        title = filename.split('/')[-1]

    if filetype == 'html':
        bokeh.io.save(fig, filename=filename, title=title, **kwargs)
    elif filetype == 'png':
        bokeh.io.export_png(fig, filename=filename, **kwargs)
    elif filetype == 'svg':
        bokeh.io.export_svgs(fig, filename=filename, **kwargs)
    else:
        raise ValueError('''
                         Please specify filetype as one of "html", "png"
                         ', or "svg".
                         ''')
