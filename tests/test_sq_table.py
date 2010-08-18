
import datetime
import decimal
import os
import time
import sys
import unittest

from .lib import sq_pack
from .lib import sq_table
from .lib.sq_exceptions import ColumnException, IndexRowTooLong, \
    IndexWarning, TooManyIndexRows

errors = (IOError, OSError)
if sys.platform.startswith('win'):
    errors = (IOError, OSError, WindowsError)

class TableAdapterTest(unittest.TestCase):
    def setUp(self):
        try:
            os.remove('test.sqlite')
        except errors:
            pass
        for i in xrange(10):
            try:
                self.table = sq_table.TableAdapter('test.sqlite', 'test_table')
            except:
                time.sleep(.1)
                continue
            else:
                break
        else:
            raise

    def tearDown(self):
        self.table.db.close()
        del self.table
        try:
            time.sleep(.2)
            os.remove('test.sqlite')
        except errors:
            pass

    def test_create_index(self):
        self.table.add_index('col1', 'col2', '-col3', 'col4')
        # verify index length
        self.assertEquals(len(list(self.table.db.execute("select * from _indexes"))), 1)
        # drop existing index
        self.table.drop_index('col1', 'col2')
        # drop non-existing index
        self.table.drop_index('col1', 'col2', '-col3', 'col4')
        # verify no indexes
        self.assertEquals(len(list(self.table.db.execute("select * from _indexes"))), 0)
        # verify that a number cannot be a prefix to a column
        self.assertRaises(ColumnException, lambda:self.table.add_index('col1', '5col'))
        self.assertRaises(ColumnException, lambda:self.table.add_index('col1', '+5col'))
        # verify that you cannot create an empty index
        self.assertRaises(IndexWarning, lambda:self.table.add_index())
        # verify table metadata sizes
        self.assertEquals(len(list(self.table.db.execute("select * from _indexes"))), 0)

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

    def test_basic_insert(self):
        self.table.add_index('col1', 'col2', 'col3')
        data = {'col1':[datetime.date.today(), 3, 1.4], 'col2':datetime.datetime.utcnow(), 'col3':decimal.Decimal('1.5')}
        rowid, index_rows, index_rows_inserted = self.table.insert(data)
        retr = self.table.data.select_one(('data',), _id=rowid)
        self.assertEquals(retr[0], data)
        data['_id'] = rowid
        data['col3'] = 5
        self.table.update(data)
        retr = self.table.data.select_one(('data',), _id=rowid)
        self.assertEquals(retr[0], data)
        self.table.delete(rowid)
        self.assertEquals(self.table.data.select_one(('data',), _id=rowid), None)

if __name__ == '__main__':
    unittest.main()
