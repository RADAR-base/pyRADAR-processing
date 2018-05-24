import os
import tables
from functools import wraps

# Pytables HDF5 default filter
_FILTER = tables.Filters(complib='blosc:snappy', complevel=1, shuffle=True)

# Schemas dir
_PACKAGE_DIR = '/' + os.path.join('', *__file__.split(os.path.sep)[:-2])
_SPECIFICATION_DIR = os.path.join(_PACKAGE_DIR, 'radar_schemas',
                                  'specifications')
_SCHEMA_DIR = os.path.join(_PACKAGE_DIR, 'radar_schemas', 'commons')
_SCHEMA_KEY_FILE = None
_DEVICE = 'android_'

# Default time columns
TIME_COLS = ('value.time', 'value.timeReceived')

# Verbosity / debug
VERBOSITY = 0
def debug_wrapper(function):
    """ Wrapper that prints debug information when radar.common.VERBOSITY
    is set above 0.
    """
    @wraps(function)
    def wrapper(*args, **kwargs):
        if not VERBOSITY:
            return function(*args, **kwargs)
        print('^^^^^^^^^^ {} from {} ^^^^^^^^^^'.format(
            function.__name__, function.__module__))
        if VERBOSITY > 1:
            print('Args: ')
            for arg in args:
                print('Class: {} | Value: {}'.format(
                    arg.__class__, arg.__str__()))

            print('KWargs: ')
            for key, value in kwargs.items():
                print('{}: Class: {} | Value: {}'.format(key, value.__class__,
                                                       value.__str__()))
        print('____________________')
        result = function(*args, **kwargs)
        print('____________________')
        if VERBOSITY > 2:
            print('Result: ')
            print('Class: {} | Value: {}'.format(
                result.__class__, result.__str__()))
        print('vvvvvvvvvv {} from {} vvvvvvvvvv'.format(function.__name__,
                                                        function.__module__))
        return result
    return wrapper

class argu_decorator(object):
    def __init__(self, *oargs, **okwargs):
        self.oargs = oargs
        self.okwargs = okwargs

    def __call__(self, func, *args, **kwargs):
        def argu_func(*args, **kwargs):
            return func(*args, *self.oargs, **kwargs, **self.okwargs)
        return argu_func
