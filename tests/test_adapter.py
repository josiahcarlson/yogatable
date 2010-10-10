
import decimal
import datetime
import unittest

from .lib import adapt

class TestAdapter(unittest.TestCase):
    def test_adapter(self):
        data = {
            'int': 1,
            'long': 98475398472836723460296L,
            'float': 1.25,
            'string': 'sagfjkg4t3',
            'unicode': unichr(4373),
            'list': [1,2,3],
            'dictionary': {'a':'b', 'c':['a', 4, 5]},
            'decimal': decimal.Decimal('1.34'),
            'date': datetime.date.today(),
            'datetime': datetime.datetime.utcnow(),
            'time': datetime.time(12, 45, 12, 19382),
            'timedelta': datetime.timedelta(days=4, seconds=23542, microseconds=14325),
            'set': set([1,2,3,'hello']),
        }
        adapted = adapt.json_adapter(data)
        loaded = adapt.json_converter(adapted)
        self.assertEquals(data, loaded)
