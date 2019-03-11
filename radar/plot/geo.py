""" Location plots using cartopy.
Cartopy requires several libraries not available through pip.
You should either install the requirements through your package manager
or install cartopy using conda.
https://scitools.org.uk/cartopy/docs/latest/installing.html
"""
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
try:
    import cartopy.crs as ccrs
    from cartopy.io.img_tiles import Stamen
except ModuleNotFoundError:
    raise ImportError(
        'Geo plotting requires cartopy\n'
        'Installing with conda is the most straightforward approach: \n'
        '    conda install -c conda-forge cartopy\n'
        'https://scitools.org.uk/cartopy/docs/latest/installing.html'
    )


def geo_scatter(latitudes, longitudes, projection=None,
                tiler=Stamen('toner-background'), zoom_level=8,
                extent=None, alpha=0.3, ax=None, fig=None, **kwargs):
    """ Latitude / longitude scatter plot
    Parameters:
        latitudes (float[n]): Numpy array of latitudes
        longitudes (float[n]): Numpy array of longitudes
        projection: A cartopy projection
        tiler: A cartopy map tiler / background. Default: Stamen terrain
        zoom_level (int): Tiler zoom level. Default: 8
        extent (float[4]): [Long_min, Long_max, Lat_min, Lat_max]
        ax: matplotlib axes object. Will create one if not given.
        fig: matplotlib figure. Will create one if not given
        alpha (float): Alpha value for scatter points
        kwargs: other kwargs to provide to matplotlib.scatter
    Returns:
        matplotlib.figure.Figure
    """
    if projection is None:
        projection = ccrs.PlateCarree()
    if ax is None:
        if fig is None:
            fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(1, 1, 1, projection=projection)
    if extent:
        ax.set_extent(extent, crs=projection)
    if tiler:
        ax.add_image(tiler, zoom_level)
    else:
        ax.coastlines('10m')
    ax.scatter(longitudes, latitudes, alpha=alpha,
               transform=projection, **kwargs)
    scale_bar(ax)
    gl = ax.gridlines(draw_labels=True, color='white')
    gl.xlabels_top = False
    gl.ylabels_right = False
    return fig


def geo_heatmap(latitudes, longitudes, projection=None,
                tiler=Stamen('toner-background'), zoom_level=8,
                extent=None, bins=40, ax=None, fig=None, **kwargs):
    """ Latitude / longitude heatmap
    Parameters:
        latitudes (float[n]): Numpy array of latitudes
        longitudes (float[n]): Numpy array of longitudes
        projection: A cartopy projection
        tiler: A cartopy map tiler / background. Default: Stamen terrain
        zoom_level (int): Tiler zoom level. Default: 8
        extent (float[4]): [Long_min, Long_max, Lat_min, Lat_max]
        bins (int): Number of bins for the heatmap
        ax: matplotlib axes object. Will create one if not given.
        fig: matplotlib figure. Will create one if not given
        kwargs: other kwargs to provide to ax.hist2d
    Returns:
        matplotlib.figure.Figure
    """
    cmap = mpl.cm.Reds
    n = cmap.N
    cmap = cmap(np.arange(n))
    cmap[:, -1] = np.linspace(0.7, 0.99, n)
    cmap[0, -1] = 0
    cmap = mpl.colors.ListedColormap(cmap)
    if projection is None:
        projection = ccrs.PlateCarree()
    if ax is None:
        if fig is None:
            fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(1, 1, 1, projection=projection)
    if not extent:
        extent = [longitudes.min()-0.1, longitudes.max()+0.1,
                  latitudes.min()-0.1, latitudes.max()+0.1]
    ax.set_extent(extent, crs=projection)
    if tiler:
        ax.add_image(tiler, zoom_level)
    else:
        ax.coastlines('10m')
    xy = ax.projection.transform_points(ccrs.Geodetic(), longitudes.values,
                                        latitudes.values)
    ax.hist2d(xy[:, 0], xy[:, 1], norm=mpl.colors.LogNorm(), cmap=cmap,
              bins=bins, zorder=10, **kwargs)
    scale_bar(ax)
    gl = ax.gridlines(draw_labels=True, color='white')
    gl.xlabels_top = False
    gl.ylabels_right = False
    return fig


def geo_hexmap(latitudes, longitudes, projection=None,
               tiler=Stamen('toner-background'), zoom_level=8,
               extent=None, gridsize=30, ax=None, fig=None, **kwargs):
    """ Latitude / longitude hexmap
    Parameters:
        latitudes (float[n]): Numpy array of latitudes
        longitudes (float[n]): Numpy array of longitudes
        projection: A cartopy projection
        tiler: A cartopy map tiler / background. Default: Stamen terrain
        zoom_level (int): Tiler zoom level. Default: 8
        extent (float[4]): [Long_min, Long_max, Lat_min, Lat_max]
        gridsize (int): Gridsize of the hexs. Default: 30
        ax: matplotlib axes object. Will create one if not given.
        fig: matplotlib figure. Will create one if not given
        kwargs: other kwargs to provide to ax.hist2d
    Returns:
        matplotlib.figure.Figure
    """
    cmap = mpl.cm.Reds
    n = cmap.N
    cmap = cmap(np.arange(n))
    cmap[:, -1] = np.linspace(0.7, 0.99, n)
    cmap[0, -1] = 0
    cmap = mpl.colors.ListedColormap(cmap)
    if projection is None:
        projection = ccrs.PlateCarree()
    if ax is None:
        if fig is None:
            fig = plt.figure(figsize=(8, 8))
        ax = fig.add_subplot(1, 1, 1, projection=projection)
    if not extent:
        extent = [longitudes.min()-0.05, longitudes.max()+0.05,
                  latitudes.min()-0.05, latitudes.max()+0.05]
    ax.set_extent(extent, crs=projection)
    if tiler:
        ax.add_image(tiler, zoom_level)
    else:
        ax.coastlines('10m')
    xy = ax.projection.transform_points(ccrs.Geodetic(), longitudes.values,
                                        latitudes.values)
    ax.hexbin(xy[:, 0], xy[:, 1], norm=mpl.colors.LogNorm(), cmap=cmap,
              gridsize=gridsize, zorder=10, edgecolors=None, **kwargs)
    scale_bar(ax)
    gl = ax.gridlines(draw_labels=True, color='white')
    gl.xlabels_top = False
    gl.ylabels_right = False
    return fig


def scale_bar(ax, length=None, location=(0.5, 0.05), linewidth=3):
    """ Scale bar for cartopy plots
    Parameters:
        ax: matplotlib axes to draw on.
        length (int): length of scalebar in km
        location (tuple(float, float)): is center of the scalebar
        linewidth (float): Thickness of the scalebar.
    Returns:
        None
    """
    llx0, llx1, lly0, lly1 = ax.get_extent(ccrs.PlateCarree())
    sbllx = (llx1 + llx0) / 2
    sblly = lly0 + (lly1 - lly0) * location[1]
    tmc = ccrs.TransverseMercator(sbllx, sblly)
    x0, x1, y0, y1 = ax.get_extent(tmc)
    sbx = x0 + (x1 - x0) * location[0]
    sby = y0 + (y1 - y0) * location[1]

    def scale_number(x):
        if str(x)[0] in ['1', '2', '5']:
            return int(x)
        return scale_number(x - 10 ** ndim)
    if not length:
        length = (x1 - x0) / 5000
        ndim = int(np.floor(np.log10(length)))
        length = round(length, -ndim)
        length = scale_number(length)

    bar_xs = [sbx - length * 500, sbx + length * 500]
    ax.plot(bar_xs, [sby, sby], transform=tmc, color='k', linewidth=linewidth)
    ax.text(sbx, sby, str(length) + ' km', transform=tmc,
            horizontalalignment='center', verticalalignment='bottom')
