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
config.schema.key = None
config.schema.dir = None
config.specifications = Config()
config.specifications.dir = None
config.io = Config()
config.io.index = 'value.time'
config.log = Config()
config.log.to_file = None


"""
# Pytables HDF5 default filter
_FILTER = tables.Filters(complib='zlib', complevel=4, shuffle=True)

# Schemas dir
_PACKAGE_DIR = '/' + os.path.join('', *__file__.split(os.path.sep)[:-2])
_SPECIFICATION_DIR = os.path.join(_PACKAGE_DIR, 'radar_schemas',
                                  'specifications')
