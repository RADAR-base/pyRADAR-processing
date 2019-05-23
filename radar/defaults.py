""" Configuration defaults and classes
Contains the config and also default schemas, specifications,
protocols, and definitions.
"""
import os
import collections
import yaml
schemas = {}
specifications = {}
protocols = {}
definitions = {}

_ROOT = os.path.abspath(os.path.dirname(__file__))
_DEF_RADAR_SCHEMA_PATH = os.path.join(_ROOT, 'Schemas', 'commons')
_DEF_RADAR_SPECS_PATH = os.path.join(_ROOT, 'Schemas', 'specifications')


class Config(dict):
    """ Class that allows access to values through getattr,
    and converts the class of setattr/setitem dicts to Config
    """
    def __getitem__(self, key):
        return dict.get(self, key, None)

    def __getattr__(self, key):
        return self.get(key, None)

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
config.log.to_file = None
config.log.format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
config.schema.from_local_file = False
config.schema.local_file_regex = '.*schema.*json'
config.specifications.dir = _DEF_RADAR_SPECS_PATH
config.specifications.git = False
config.specifications.github_owner = 'RADAR-base'
config.specifications.github_repo = 'RADAR-Schemas'
config.specifications.github_sha = 'extended-specs'
config.io.index = 'time'
config.protocol.url = ''
config.project.ptckw = {}
config.project.datakw = {}


def update(d, u):
    """ update nested mappings
    d (dict): mapping to update
    u (dict): mapping to update with
    """
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            d[k] = update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


_CONFIG_DEFAULT_FILE = os.path.join(_ROOT, 'config.yml')
if os.path.exists(_CONFIG_DEFAULT_FILE):
    with open(_CONFIG_DEFAULT_FILE) as f:
        update(config, yaml.load(f, Loader=yaml.FullLoader))

if 'config.yml' in os.listdir():
    with open('config.yml', 'r') as f:
        update(config, yaml.load(f, Loader=yaml.FullLoader))
