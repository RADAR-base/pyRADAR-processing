#!/usr/bin/env python3
import pandas as pd
from scipy import signal

def butterworth(arr, cutoff, freq, order=5, ftype='highpass'):
    """ butterworth filters the array through a two-pass butterworth filter.
    Parameters
    __________
    arr: list or numpy.array
        The timeseries array on which the filter is applied
    cutoff: scalar or len-2 sequence
        The critical frequencies for the Butterworth filter. The point at
        which the gain drops to 1/sqrt(2) of the passband. Must be length-2
        sequence for a bandpass filter giving [low, high]. Or else the scalar
        cutoff frequency for either a low-pass' or 'highpass' filter.
    freq: float
        The frequency (Hz) of the input array
    ftype: {'highpass', 'lowpass', 'bandpass'}
        The filter type. Default is 'highpass'
    """
    nyq = 0.5 * freq
    b, a = signal.butter(order, cutoff/nyq, ftype)
    return signal.filtfilt(b, a, arr)

def accel_linear(accel_array, freq):
    """ acceleration_gravity runs a filter on the input array(s) to remove the
    gravitational component and return linear acceleration. Uses a two-pass
    Butterworth filter with cutoff = 0.5Hz, order=5
    Parameters
    __________
    accel_array : list(s) or numpy array(s)
        The acceleration data that the filter will be applied to. Can be single
        or multiple vectors. Each dimension is treated as a seperate
        accelerometer vector. Vectors should be in rows for numpy arrays but
        columns for pandas DataFrames
    freq: float
        The frequency (Hz) of the accelerometer
    Returns
    _______
    linear_accel: type(accel_array)
        The linear acceleration as predicted by the highpass filter
    """
    if accel_array.ndim > 1:
        linear_accel = accel_array.copy()
        if isinstance(accel_array, pd.DataFrame):
            for col in linear_accel:
                linear_accel[col] = butterworth(linear_accel[col], cutoff=0.5,
                                                freq=freq, ftype='highpass')
        else:
            for arr in linear_accel:
                arr = butterworth(arr, cutoff=0.5, freq=freq, ftype='highpass')

    else:
        linear_accel = butterworth(accel_array, cutoff=0.5, freq=freq,
                                   ftype='highpass')
    return linear_accel

def accel_gravity(accel_array, freq):
    """ accel_gravity runs a filter on the input array(s) to extract
    the gravitational component. Uses a two-pass Butterworth filter with
    cutoff = 0.5Hz, order=5
    Parameters
    __________
    accel_array : list(s) or numpy array(s)
        The acceleration data that the filter will be applied to. Can be single
        or multiple vectors. Each dimension is treated as a seperate
        accelerometer vector. Vectors should be in rows for numpy arrays but
        columns for pandas DataFrames
    freq: float
        The frequency (Hz) of the accelerometer
    Returns
    _______
    gravity: type(accel_array)
        The gravity as predicted by the lowpass filter
    """
    if accel_array.ndim > 1:
        gravity = accel_array.copy()
        if isinstance(accel_array, pd.DataFrame):
            for col in gravity:
                gravity[col] = butterworth(gravity[col], cutoff=0.5,
                                           freq=freq, ftype='lowpass')
        else:
            for arr in gravity:
                arr = butterworth(arr, cutoff=0.5, freq=freq, ftype='lowpass')

    else:
        gravity = butterworth(accel_array, cutoff=0.5, freq=freq,
                              ftype='lowpass')
    return gravity
