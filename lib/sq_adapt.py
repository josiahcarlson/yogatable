
from datetime import datetime, date, time
from decimal import Decimal as decimal
import itertools

import json
import sqlite3

# decimal, date, datetime

def _check(v, d):
    if v.tzinfo:
        raise ValueError('Only serialize naive times! We cannot JSON serialize timezones.')
    return d

ADAPTERS = {
    datetime: (lambda v: _check(v, {'__datetime':tuple(v.timetuple())[:6]+(v.microsecond,)})),
    date: lambda v: {'__date':[v.year, v.month, v.day]},
    decimal: lambda v: {'__decimal':str(v)},
    time: (lambda v: _check(v, {'__time':[v.hour, v.minute, v.second, v.microsecond]})),
}

CONVERTERS = {
    '__datetime': lambda v: datetime(*v),
    '__date': lambda v: date(*v),
    '__decimal': decimal,
    '__time': lambda v: time(*v)
}

def _json_adapter(v):
    adapt = ADAPTERS.get(type(v))
    if adapt:
        return adapt(v)
    raise TypeError("can't adapt %r"%(v,))

def _json_converter(v):
    # todo: benchmark checking length before iterating.
    if len(v) == 1:
        for key, val in v.iteritems():
            # Adapted datatypes should only have one __key, so this works
            # correctly for those.  Non-adapted datatypes shouldn't have any
            # equivalent keys to what we adapted, so we can break after one pass.
            conv = CONVERTERS.get(key)
            if conv:
                return conv(val)
            break
    if v:
        return dict(itertools.izip(itertools.imap(str, v.iterkeys()), v.itervalues()))
    return v

def json_adapter(dictionary):
    return buffer(json.dumps(dictionary, default=_json_adapter, separators=(',',':')))

def json_converter(data):
    return json.loads(str(data), object_hook=_json_converter)

sqlite3.register_adapter(dict, json_adapter)
# we may not want to decode on read... save that for the final client
sqlite3.register_converter('JSON', json_converter)
sqlite3.register_converter('BLOB', str)
