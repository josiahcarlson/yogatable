
from collections import defaultdict
import itertools
import pprint
import time
import urllib
import urllib2
import uuid

from .lib import adapt
from .lib.exceptions import BadResponseCode

def new_uuid(counter=iter(itertools.count())):
    return str(uuid.uuid1(None, counter.next()))

class SimpleYogaClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
    def __getattr__(self, name):
        return Table(self.host, self.port, name)

class Table(object):
    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name
    def __getattr__(self, method):
        return Operation(self.host, self.port, self.name, method)

class Operation(object):
    def __init__(self, host, port, table, method):
        self.host = host
        self.port = port
        self.table = table
        self.method = method

    def __call__(self, *args, **kwargs):
        rid = new_uuid()
        data = adapt.json_adapter([args, kwargs])
        post = urllib.urlencode([('args', data), ('table', self.table), ('rid', rid)])
        url = 'http://%s:%s/%s?%s'%(self.host, self.port, self.method, post)
        result = urllib2.urlopen(url)
        if result.code != 200:
            raise BadResponseCode("Expected 200 code, got %r instead", result.code)

        response = adapt.json_converter(result.read())
        exceptions.check_response(response)
        assert response['response'] == 'ok'
        assert response['rid'] == rid
        return response['value']

if __name__ == '__main__':
    c = SimpleYogaClient('127.0.0.1', 8765)
    dts = []
    key = c.table.get_drop_key()
    c.table.drop_table(key)
    for i in xrange(1):
        t = time.time()
        _ = c.table.insert([{'i':j} for j in xrange(1)])
        dts.append(time.time()-t)
    hist = defaultdict(int)
    for dt in dts:
        hist[int(dt*1000)] += 1
    s = sum(dts)
    a = s / len(dts)
    ss = sum(dt * dt for dt in dts)
    print min(dts), max(dts), s / len(dts), ((ss - a*a) / len(dts))**.5
    for dti, c in sorted(hist.items()):
        print dti, c
