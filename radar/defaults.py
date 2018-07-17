import os
schemas = {}
specifications = {}
protocols = {}
definitions = {}

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
config.schema = {}
config.specifications = {}
config.schema.device = 'android'
config.schema.key = None
config.schema.dir = '/home/ubuntu/RADAR-Schemas/commons'
config.schema.git = False
config.schema.github_owner = 'RADAR-base'
config.schema.github_repo = 'RADAR-Schemas'
config.schema.github_sha = None
config.schema.read_csvs = True
config.specifications.dir = None
config.specifications.git = False
config.specifications.github_owner = 'RADAR-base'
config.specifications.github_repo = 'RADAR-Schemas'
config.specifications.github_sha = 'extended-specs'
config.io.index = 'value.time'
config.log.to_file = None

