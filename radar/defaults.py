import os
import yaml
schemas = {}
specifications = {}
protocols = {}
definitions = {}

_ROOT = os.path.abspath(os.path.dirname(__file__))
_DEF_RADAR_SCHEMA_PATH = os.path.join(_ROOT, 'Schemas', 'commons')
_DEF_RADAR_SPECS_PATH = os.path.join(_ROOT, 'Schemas', 'specifications')

# Config
class Config(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self, key) if key in self else None

    def __getattr__(self, key):
        if key in self:
            return self[key]

    def __setattr__(self, key, val):
        self[key] = val

    def __setitem__(self, key, val):
        if isinstance(val, dict):
            val = Config(val)
        dict.__setitem__(self, key, val)


config = Config()
config.aRMT = {}
config.io = {}
config.log = {}
config.protocol = {}
config.schema = {}
config.specifications = {}
config.project = {}
config.schema.device = 'android'
config.schema.key = None
config.schema.dir = _DEF_RADAR_SCHEMA_PATH
config.schema.git = False
config.schema.github_owner = 'RADAR-base'
config.schema.github_repo = 'RADAR-Schemas'
config.schema.github_sha = None
config.schema.read_csvs = True
config.specifications.dir = _DEF_RADAR_SPECS_PATH
config.specifications.git = False
config.specifications.github_owner = 'RADAR-base'
config.specifications.github_repo = 'RADAR-Schemas'
config.specifications.github_sha = 'extended-specs'
config.io.index = 'time'
config.log.to_file = None
config.protocol.url = ''
config.project.ptckw = {}
config.project.datakw = {}


import collections
def update(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


_CONFIG_DEFAULT_FILE = os.path.join(_ROOT, 'config.yml')
if os.path.exists(_CONFIG_DEFAULT_FILE):
    with open(_CONFIG_DEFAULT_FILE) as f:
        update(config, yaml.load(f))

if 'config.yml' in os.listdir():
    with open('config.yml', 'r') as f:
        update(config, yaml.load(f))
