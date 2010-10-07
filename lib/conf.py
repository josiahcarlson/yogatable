
import imp

from lib import default_config

class AttrDict(dict):
    def __getattr__(self, attr):
        return self[attr]
    def __setattr__(self, attr, val):
        self[attr] = val

class Config(object):
    def __init__(self, *modules):
        self.config = list(modules)
        self.cached = {}
        self.cached_tables = {}
    def add(self, module):
        self.config.append(module)
        self.cached.clear()
        self.cached_tables.clear()
    def __getattr__(self, name):
        if name not in self.cached:
            for config in reversed(self.config):
                if hasattr(config, name):
                    self.cached[name] = getattr(config, name)
                    break
            else:
                raise AttributeError("cannot find configuration option %r"%(name,))
        return self.cached[name]
    def table_config(self, table):
        if table not in self.cached_tables:
            new = []
            for conf in self.config:
                new.append(conf)
                if hasattr(conf, 'TABLE_CONFIGURATION') and table in conf.TABLE_CONFIGURATION:
                    new.append(AttrDict(conf.TABLE_CONFIGURATION[table]))
            self.cached_tables[table] = Config(*new)
        return self.cached_tables[table]

def load_settings(path):
    conf = Config(default_config)
    if path:
        conf.add(imp.load_module('_config', open(path, 'U'), path, ('.py', 'U', 1)))
    return conf
