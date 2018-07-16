import os

class Config(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key) if key in self else None

    def __getattr__(self, key):
        if key in self:
            return self[key]

    def __setattr__(self, key, val):
        self[key] = val

config = Config()
config.schema = Config()
config.schema.device = 'android'
config.io = Config()
config.io.index = 'value.time'


"""
# Pytables HDF5 default filter
_FILTER = tables.Filters(complib='zlib', complevel=4, shuffle=True)

# Schemas dir
_PACKAGE_DIR = '/' + os.path.join('', *__file__.split(os.path.sep)[:-2])
_SPECIFICATION_DIR = os.path.join(_PACKAGE_DIR, 'radar_schemas',
                                  'specifications')
_SCHEMA_DIR = os.path.join(_PACKAGE_DIR, 'radar_schemas', 'commons')
_SCHEMA_KEY_FILE = None
_DEVICE = 'android_'

# Default time columns
TIME_COLS = ('value.time', 'value.timeReceived')

# Logging
FILE_LOGGING = False
"""
