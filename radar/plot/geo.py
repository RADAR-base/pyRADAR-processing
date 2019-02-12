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
                tiler=Stamen('terrain-background'), zoom_level=8,
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
            fig = plt.figure(figsize=(10, 5))
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
                tiler=Stamen('toner-lite'), zoom_level=8,
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
        kwargs: other kwargs to provide to matplotlib.scatter
    Returns:
        matplotlib.figure.Figure
    """
    cmap = mpl.cm.Greens
    n = cmap.N
    cmap = cmap(np.arange(n))
    cmap[:, -1] = np.linspace(0.4, 0.9, n)
    cmap = mpl.colors.ListedColormap(cmap)
    if projection is None:
        projection = ccrs.PlateCarree()
    if ax is None:
        if fig is None:
            fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot(1, 1, 1, projection=projection)
    if extent:
        ax.set_extent(extent, crs=projection)
    if tiler:
        ax.add_image(tiler, zoom_level)
    else:
        ax.coastlines('10m')
    xy = ax.projection.transform_points(ccrs.Geodetic(), longitudes.values,
                                        latitudes.values)
    ax.hist2d(xy[:, 0], xy[:, 1], norm=mpl.colors.LogNorm(), cmap=cmap,
              bins=bins, zorder=10)
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
