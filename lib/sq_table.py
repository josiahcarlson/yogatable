
from __future__ import with_statement

import bisect
import collections
import itertools
import re
import sqlite3
import uuid

from sq_exceptions import BAD_NAMES, ColumnException, IndexError, IndexWarning
from sq_om import DataTable, IndexInfo, IndexTable
from sq_pack import generate_index_rows

PAGE_SIZES = (512, 1024, 2048, 4096, 8192, 16384, 32768)

COL_REGEX = re.compile('^[-+]?[a-z_][a-z0-9_]*[-+]?$')

def _select_clause(table_name):
    return "SELECT *, rowid FROM %s "%(table_name,)

def bisect_contains(lst, item):
    index = bisect.bisect_left(lst, item)
    return index if (index < len(lst) and lst[index] == item) else None

def new_uuid(counter=iter(itertools.count())):
    return str(uuid.uuid1(None, counter.next()))

class TableAdapter(object):
    class INDEX_FLAGS:
        pending = 0x1
        unique = 0x2
    def __init__(self, dbfile, tablename, *args, **kwargs):
        # todo: should probably replace the sqlite3 connect with a passed
        # backend parameter
        kwargs['detect_types'] = sqlite3.PARSE_DECLTYPES
        page_size = kwargs.pop('page_size', 8192)
        assert page_size in PAGE_SIZES
        self.db = sqlite3.connect(dbfile, *args, **kwargs)
        self.db.execute('PRAGMA page_size = %i'%(page_size,))
        self.db.execute('PRAGMA foreign_keys = ON;').fetchone()
        self.table = tablename
        self.known_indexes = []
        self.indexes_to_ids = {}
        self.indexes_in_progress = collections.deque()
        self.setup()

    def _get_freelist_count(self):
        return self.db.execute('PRAGMA freelist_count;').fetchone()[0]

    def setup(self):
        # create the index listing if it doesn't exist
        # todo: add support for unique index
        self.indexes = IndexInfo(self.db)

        # handle this table's information
        self.data = DataTable(self.db)
        self.index = IndexTable(self.db)

        # cache the known set of indexes
        indexes = self.indexes.select(('index_id', 'columns', 'flags'))
        for index_id, columns, flags in indexes:
            if flags & self.INDEX_FLAGS.pending:
                self.indexes_in_progress.append(index_id)
            self.known_indexes.append(columns)
            self.indexes_to_ids[columns] = index_id

        self.known_indexes.sort()

    def _col_def(self, columns):
        # check for a valid index
        if not columns:
            raise IndexWarning("Cannot create null index")
        # check for valid column names
        columns = list(columns)
        for i, column in enumerate(columns):
            if not COL_REGEX.match(column) or column in BAD_NAMES:
                raise ColumnException("Bad column name: %r", column)
            columns[i] = column.strip('+')

        if len(columns) != len(set(col.strip('-') for col in columns)):
            raise IndexError("Cannot list the same column twice in an index")

        return ','.join(columns) + ','

    def insert(self, data, cursor=None):
        # get the rows to index first
        row_count, index_rows = generate_index_rows(data, self.indexes_to_ids)
        if '_id' not in data:
            data['_id'] = new_uuid()
        rowref = data['_id']
        index_data = zip(index_rows, itertools.repeat(rowref))
        # insert the data, then insert the index rows
        if cursor:
            self.data.insert(data, conn=cursor)
            self.index.insert_many(index_data, conn=cursor)
        else:
            with self.db as cursor:
                self.data.insert(data, conn=cursor)
                self.index.insert_many(index_data, conn=cursor)
        return rowref, row_count, len(index_rows)

    def insert_many(self, data, conn=None):
        conn = conn or self.db
        with conn as cursor:
            return map(self.insert, zip(data, itertools.repeat(cursor)))

    def update(self, data):
        rowref = data.pop('_id')
        existing_keys = dict(self.index.select(('idata', 'rowid'), rowref=rowref))
        old_keys = set(existing_keys)
        count, new_keys = generate_index_rows(data, self.indexes_to_ids)
        new_keys = set(new_keys)
        to_remove = old_keys - new_keys
        to_add = new_keys - old_keys

        with self.db as cursor:
            self.data.update(data, rowref, conn=cursor)
            if to_remove:
                self.index.delete(rowid=sorted(existing_keys[key] for key in to_remove), conn=cursor)
            if to_add:
                self.index.insert_many(zip(to_add, itertools.repeat(rowref)), conn=cursor)

    def delete(self, id, cursor=None):
        if cursor:
            self.data.delete(_id=id, conn=cursor)
            self.index.delete(rowref=id, conn=cursor)
        else:
            with self.db as cursor:
                self.data.delete(_id=id, conn=cursor)
                self.index.delete(rowref=id, conn=cursor)

    def delete_many(self, ids):
        with self.db as cursor:
            self.data.delete(_id=ids, conn=cursor)
            self.index.delete(rowref=ids, conn=cursor)

    def add_index(self, *columns):
        index_def = self._col_def(columns)
        # check for duplicate indexes
        index_check = bisect.bisect_left(self.known_indexes, index_def)
        if index_check < len(self.known_indexes):
            if self.known_indexes[index_check].startswith(index_def):
                raise IndexWarning("New index %r is a prefix of existing index %r",
                    index_def, self.known_indexes[index_check])

        # push the index changes to the backend
        index_id = self.indexes.select_one(("max(index_id)",))
        index_id = index_id[0] if index_id else None
        index_id = 0 if index_id is None else index_id + 1
        self.indexes.insert((index_id, index_def, 1, 0.0), "OR ROLLBACK")

        # add to the local cached listing
        self.indexes_to_ids[index_def] = index_id
        bisect.insort_left(self.known_indexes, index_def)

    def drop_index(self, *columns):
        index_def = self._col_def(columns)

        row = self.indexes.select_one(('index_id', 'rowid'), columns=index_def)
        if row:
            index_id, rowid = row
            self.indexes.delete(rowid=rowid)

        if index_def in self.indexes_to_ids:
            del self.indexes_to_ids[index_def]
            self.known_indexes.remove(index_def)
