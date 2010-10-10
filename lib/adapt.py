
from datetime import datetime, date, time, timedelta
from decimal import Decimal as decimal
import itertools

import json
import sqlite3

def _check(v, d):
    if v.tzinfo:
        raise ValueError('Only serialize naive times! We cannot JSON serialize timezones.')
    return d

ADAPTERS = {
    datetime: (lambda v: _check(v, {'__datetime':tuple(v.timetuple())[:6]+(v.microsecond,)})),
    date: lambda v: {'__date':[v.year, v.month, v.day]},
    decimal: lambda v: {'__decimal':str(v)},
    set: lambda v: {'__set': list(v)},
    time: (lambda v: _check(v, {'__time':[v.hour, v.minute, v.second, v.microsecond]})),
    timedelta: (lambda v: {'__timedelta':[v.days, v.seconds, v.microseconds]}),
}

CONVERTERS = {
    '__datetime': lambda v: datetime(*v),
    '__date': lambda v: date(*v),
    '__decimal': decimal,
    '__set': lambda v: set(v),
    '__time': lambda v: time(*v),
    '__timedelta': lambda v: timedelta(days=v[0], seconds=v[1], microseconds=v[2]),
}
CONVERTERS_SET = set(CONVERTERS)

def _json_adapter(v):
    adapt = ADAPTERS.get(type(v))
    if adapt:
        return adapt(v)
    raise TypeError("can't adapt %r"%(v,))

'''
Given...
def v0(d):
    if len(d) == 1:
        for k,v in d.iteritems():
            op = CONVERTERS.get(k)
            if op:
                return op(v)
            break
    return d

def v1(d):
    if len(d) == 1:
        for k,v in d.iteritems():
            if k in CONVERTERS:
                return CONVERTERS[k](v)
            break
    return d

def v2(d):
    for k,v in d.iteritems():
        if k in CONVERTERS:
            return CONVERTERS[k](v)
        break
    return d

def v3(d):
    if len(d) == 1 and set(d) & CONVERTERS_SET:
        k,v = d.popitem()
        return CONVERTERS[k](v)
    return d

We find that v3 is the fastest over all cases...

>>> benchmark_nonmatching_dict(10000000)
v0 2.28099989891
v1 2.49900007248
v2 3.64100003242
v3 2.24699997902
>>> benchmark_matching_dict(10000000)
v0 8.33500003815
v1 7.55500006676
v2 6.15799999237
v3 2.2460000515
>>> benchmark_large_dict(10000000)
v0 2.27300000191
v1 2.49499988556
v2 6.18400001526
v3 2.26200008392
'''


def _json_converter(v):
    if len(v) == 1 and set(v) & CONVERTERS_SET:
        # See the above why this version was chosen.
        # All adapted datatypes only have one __key, which is in the
        # CONVERTERS_SET.
        k, v = v.popitem()
        return CONVERTERS[k](v)
    elif v:
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
