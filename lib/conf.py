
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
    def add(self, module):
        self.config.append(module)
    def _find(self, name):
        i = len(self.config) - 1
        while i >= 0:
            if hasattr(self.config[i], name):
                return getattr(self.config[i], name)
            i -= 1
        raise AttributeError("cannot find configuration option %r"%(name,))
    def __getattr__(self, name):
        return self._find(name)
    def table_config(self, table):
        new = []
        for conf in self.config:
            new.append(conf)
            if hasattr(conf, 'TABLE_CONFIGURATION') and table in conf.TABLE_CONFIGURATION:
                new.append(AttrDict(conf.TABLE_CONFIGURATION[table]))
        return Config(*new)

def load_settings(path):
    conf = Config(default_config)
    if path:
        conf.add(imp.load_module('_config', open(path, 'U'), path, ('.py', 'U', 1)))
    return conf
