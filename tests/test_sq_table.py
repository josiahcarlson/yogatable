
import datetime
import decimal
import sys
import time
import unittest

from .lib import sq_pack
from .lib import sq_table
from .lib.sq_exceptions import ColumnException, IndexRowTooLong, \
    IndexWarning, TooManyIndexRows


class TableAdapterTest(unittest.TestCase):
    def setUp(self):
        self.table = sq_table.TableAdapter('test_table.sqlite', 'test_table')
        # get rid of the old data
        self.tearDown()
        self.table = sq_table.TableAdapter('test_table.sqlite', 'test_table')

    def tearDown(self):
        self.table.drop_table(self.table.get_drop_key())
        del self.table

    def test_create_index(self):
        self.table.add_index('col1', 'col2', '-col3', 'col4')
        # verify index length
        self.assertEquals(len(list(self.table.db.execute("select * from _indexes"))), 1)
        # drop existing index
        self.table.drop_index('col1', 'col2')
        # drop non-existing index
        self.table.drop_index('col1', 'col2', '-col3', 'col4')
        # index needs to be deleted by a processor, should be one here
        self.assertEquals(len(list(self.table.db.execute("select * from _indexes"))), 1)
        # verify that a number cannot be a prefix to a column
        self.assertRaises(ColumnException, lambda:self.table.add_index('col1', '5col'))
        self.assertRaises(ColumnException, lambda:self.table.add_index('col1', '+5col'))
        # verify that you cannot create an empty index
        self.assertRaises(IndexWarning, lambda:self.table.add_index())
        # verify table metadata sizes
        self.assertEquals(len(list(self.table.db.execute("select * from _indexes"))), 1)

    def test_index_rows(self):
        data = {'col1':1, 'col2':range(10), 'col3':6}
        self.table.add_index('col1', 'col2', 'col3')
        self.assertEquals(len(sq_pack.generate_index_rows(data, self.table.indexes_to_ids)[1]), 10)
        self.assertRaises(TooManyIndexRows, lambda: sq_pack.generate_index_rows(
            {'col1':range(10), 'col2':range(3), 'col3':range(4)},
            self.table.indexes_to_ids, max_row_count=100)
        )
        self.assertEquals(len(sq_pack.generate_index_rows(
            {'col1':range(10), 'col2':range(5), 'col3':range(4)},
            self.table.indexes_to_ids, max_row_count=201)[1]),
            200
        )
        self.assertRaises(IndexRowTooLong, lambda: sq_pack.generate_index_rows(
            {'col1':100*'1', 'col2':100*'2', 'col3':100*'3'},
            self.table.indexes_to_ids, row_over_size=sq_pack.FAIL)
        )
        self.assertEquals(sq_pack.generate_index_rows(
            {'col1':100*'1', 'col2':100*'2', 'col3':100*'3'},
            self.table.indexes_to_ids, row_over_size=sq_pack.DISCARD),
            (1, [])
        )
        self.assertEquals(sq_pack.generate_index_rows(
            {'col1':100*'1', 'col2':100*'2', 'col3':100*'3'},
            self.table.indexes_to_ids, row_over_size=sq_pack.TRUNCATE)[1][0][2:],
            ''.join(sq_pack.pack([100*'1',100*'2',100*'3']))[:256]
        )
        data['_id'] = self.table.insert(data)[0]
        self.assertEquals(self.table.search([('col1', '=', 1)]), [data])
        self.assertEquals(self.table.count([('col1', '=', 1)]), 1)

    def test_basic(self):
        self.table.add_index('col1', 'col2', 'col3')
        self.table.add_index('col1', 'col3')
        data = {'col1':[datetime.date.today(), 3, 1.4], 'col2':datetime.datetime.utcnow(), 'col3':decimal.Decimal('1.5')}
        rowid, index_rows, index_rows_inserted = self.table.insert(data)
        data['_id'] = rowid
        self.assertEquals(self.table.get(rowid), data)
        self.assertEquals(self.table.search([('col1', '=', 3), ('col3', '=', decimal.Decimal('1.5'))]), [data])
        self.assertEquals(self.table.search([('col1', '=', 3), ('col3', '<', decimal.Decimal('2.0'))]), [data])
        data['col1'] = 3
        data['col3'] = 5
        self.table.update(data)
        self.assertEquals(self.table.get(rowid), data)
        self.assertEquals(self.table.search([('col1', '=', 3), ('col3', '>', 4)]), [data])
        self.table.delete(rowid)
        self.assertEquals(self.table.get(rowid), None)

    def _test_insert_performance(self):
        data = {'col1': 1, 'col2':'hey!', 'col3': datetime.datetime.utcnow()}
        _data = [[dict(data) for i in xrange(5000)] for j in xrange(1)]
        count = sum(map(len, _data))
        t = time.time()
        for _d in _data:
            self.table.insert(_d)
        print >>sys.stderr, '\n',
        print >>sys.stderr, count / (time.time()-t)
        self.assertEquals(list(self.table.db.execute('select count(*) from _data'))[0][0], count)
        t = time.time()
        for i in xrange(1000):
            self.table.insert(data)
        print >>sys.stderr, 1000 / (time.time()-t)
        self.assertEquals(list(self.table.db.execute('select count(*) from _data'))[0][0], count + 1000)
