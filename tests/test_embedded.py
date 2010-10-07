
import os
import sys
import time
import unittest

import embedded
from .lib import default_config
from .lib import exceptions

class TestEmbedded(unittest.TestCase):
    def setUp(self):
        try:
            os.stat('test.sqlite')
        except:
            pass
        else:
            os.unlink('test.sqlite')
        self.db = embedded.Database(default_config)

    def tearDown(self):
        self.db.test.drop_table(self.db.test.get_drop_key())
        self.db.shutdown_with_kill()

    def test_base(self):
        id = self.db.test.insert({'i':1})[0]
        self.assertEquals(self.db.test.get(id), {'i':1, '_id':id})
        self.db.test.add_index('i')
        time.sleep(1) # let the data index
        self.assertEquals(self.db.test.search([('i', '=', 1)]), [{'i':1, '_id':id}])

    def test_index_auto(self):
        d = [{'i':i} for i in xrange(1000)]
        self.db.test.insert(d)
        self.db.test.add_index('i')
        while 1:
            result = self.db.test.search([('i', '>', -1)], ('-i',), 1)
            if result:
                if result[0]['i'] < 999:
                    print >>sys.stderr, 'indexing progress:', result[0]['i']+1, "/ 1000"
                else:
                    break
            time.sleep(1)
        self.db.test.drop_index('i')
        while 1:
            try:
                result = self.db.test.search([('i', '>', -1)], ('i',), 1)
            except exceptions.TableIndexError:
                break
            if result:
                if result[0]['i'] < 999:
                    print >>sys.stderr, 'deletion progress:', result[0]['i']+1, "/ 1000"
                else:
                    break
            time.sleep(1)

    def test_basic_index(self):
        self.db.test.add_index('i')
        d = [{'i':i} for i in xrange(1000)]
        self.db.test.insert(d)
        self.assertEquals(len(self.db.test.search([('i', '=', 5)])), 1)
        self.assertEquals(len(self.db.test.search([('i', '>=', 5), ('i', '<', 10)])), 5)
        self.assertEquals(self.db.test.search([('i', '>', 100)], limit=1)[0]['i'], 101)
        self.assertEquals(self.db.test.search([('i', '<', 900)], ('-i',), 1)[0]['i'], 899)

    def _test_multi_column(self):
        d = [{'i':int(i//10), 'j':i+23, 'k':-i} for i in xrange(1000)]
        self.db.test.insert(d)
        self.assertEquals(self.db.test.count([('i', '=', 5)]), 10)
        self.assertEquals(len(self.db.test.search([('i', '=', 5)])), 10)

        self.assertEquals(self.db.test.count([('i', '=', 24), ('j', '<', 280)]), 10)
        self.assertEquals(self.db.test.count([('i', '=', 25), ('j', '<', 280)]), 7)
        self.assertEquals(len(self.db.test.search([('i', '=', 25), ('j', '<', 280)])), 7)
        self.assertEquals(self.db.test.search([('i', '>', 90)], limit=1)[0]['i'], 91)
        self.assertEquals(self.db.test.search([('i', '<', 90)], ('-i',), 1)[0]['i'], 89)
        self.assertEquals(self.db.test.count([('i', '=', 35), ('j', '=', 373), ('k', '<', -300)]), 1)

    def test_multi_column1(self):
        self.db.test.add_index('i', '-j', 'k')
        self._test_multi_column()

    def test_multi_column2(self):
        self.db.test.add_index('i', 'j', 'k')
        self.test_multi_column1()

    def test_multi_column3(self):
        self.db.test.add_index('i', 'j', '-k')
        self._test_multi_column()

    def test_missing_indexes(self):
        # some of this is borrowed from test_table.py
        self.db.test.add_index('col1', 'col2', '-col3', 'col4')
        # drop non-existing index
        self.db.test.drop_index('col1', 'col2')
        # drop existing index
        self.db.test.drop_index('col1', 'col2', '-col3', 'col4')
        # verify that a number cannot be a prefix to a column
        self.assertRaises(exceptions.ColumnException, lambda:self.db.test.add_index('col1', '5col'))
        self.assertRaises(exceptions.ColumnException, lambda:self.db.test.add_index('col1', '+5col'))
        # verify that you cannot create an empty index
        self.assertRaises(exceptions.IndexWarning, lambda:self.db.test.add_index())

    def test_info(self):
        inf = self.db.test.info()
        self.assertTrue(inf['disk_size'] > 0)
        self.assertTrue(not inf['indexes_del'])
        self.assertTrue(not inf['indexes_add'])
        self.assertTrue(not inf['indexes'])
        self.assertTrue(inf['total_size'] > 0)
        self.assertTrue(inf['page_count'] > 0)
        self.assertTrue(inf['page_size'] > 0)
        self.assertTrue(inf['freelist_count'] == 0)
        self.assertTrue(inf['unused_size'] == 0)
        self.assertTrue(inf['cache_size'] == default_config.CACHE_SIZE)
        self.assertTrue(inf['auto_vacuum'] == default_config.AUTOVACUUM)

class TestAutovacuum(unittest.TestCase):
    def setUp(self):
        try:
            os.stat('test.sqlite')
        except:
            pass
        else:
            os.unlink('test.sqlite')
        default_config.AUTOVACUUM = 2
        default_config.MINIMUM_VACUUM_BLOCKS = 10
        self.db = embedded.Database(default_config)

    def tearDown(self):
        global default_config
        default_config = reload(default_config)
        self.db.test.drop_table(self.db.test.get_drop_key())
        self.db.shutdown_with_kill()

    def test_autovacuum(self):
        data = 8192*'1'
        d = [{'i':i, 'data':data} for i in xrange(1000)]
        r = self.db.test.insert(d)
        for ri, di in zip(r,d):
            di['_id'] = ri
        self.db.test.delete([dd['_id'] for dd in d])
        def fr():
            return self.db.test.info()['freelist_count']
        start = free = fr()
        while free >= default_config.MINIMUM_VACUUM_BLOCKS:
            print >>sys.stderr, "autovacuum progress: %i / %i" % (start-free, start)
            time.sleep(1)
            free = fr()
