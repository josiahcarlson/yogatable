
from __future__ import with_statement

import bisect
from contextlib import contextmanager
import itertools
import os
import sqlite3
import sys
import time
import uuid

from .lib.sq_exceptions import BAD_NAMES, ColumnException, IndexWarning, TableIndexError
from .lib.sq_om import DataTable, IndexInfo, IndexTable
from .lib.sq_pack import generate_index_rows, pack, Some
from .lib.sq_sqlgen import _add_one, COL_REGEX, filter_prefix, filter_prefixes, SQLGen

errors = (IOError, OSError)
if sys.platform.startswith('win'):
    errors = (IOError, OSError, WindowsError)

PAGE_SIZES = (512, 1024, 2048, 4096, 8192, 16384, 32768)

def _select_clause(table_name):
    return "SELECT *, rowid FROM %s "%(table_name,)

def bisect_contains(lst, item):
    index = bisect.bisect_left(lst, item)
    return index if (index < len(lst) and lst[index] == item) else None

def new_uuid(counter=iter(itertools.count())):
    return str(uuid.uuid1(None, counter.next()))

@contextmanager
def _cursor(cursor):
    if hasattr(cursor, '__enter__'):
        with cursor as cur:
            yield cur
    else:
        yield cursor

def _index_rows(data, indexes_to_ids):
    # get the rows to index first
    row_count, index_rows = generate_index_rows(data, indexes_to_ids)
    if '_id' not in data:
        data['_id'] = new_uuid()
    rowref = data['_id']
    index_data = zip(index_rows, itertools.repeat(rowref))
    return rowref, row_count, index_data

class TableAdapter(object):
    class INDEX_FLAGS:
        deleting = 0x1
    def __init__(self, dbfile, tablename, *args, **kwargs):
        # todo: should probably replace the sqlite3 connect with a passed
        # backend parameter
        kwargs['detect_types'] = sqlite3.PARSE_DECLTYPES
        page_size = kwargs.pop('page_size', 8192)
        assert page_size in PAGE_SIZES
        self.dbfile = dbfile
        self.db = sqlite3.connect(dbfile, *args, **kwargs)
        self.db.execute('PRAGMA page_size = %i'%(page_size,))
        self.db.execute('PRAGMA foreign_keys = ON;').fetchone()
        self.table = tablename
        self.drop_key = object()
        self._setup()

    def _get_freelist_count(self):
        return self.db.execute('PRAGMA freelist_count;').fetchone()[0]

    def _setup(self):
        # create the index listing if it doesn't exist
        # todo: add support for unique index
        self.indexes = IndexInfo(self.db)

        # handle this table's information
        self.data = DataTable(self.db)
        self.index = IndexTable(self.db)
        self._refresh_indexes()

    def _refresh_indexes(self):
        # cache the known set of indexes
        self.known_indexes = []
        self.indexes_to_ids = {}
        self.indexes_in_progress = []
        self.indexes_being_removed = []

        indexes = self.indexes.select(('index_id', 'columns', 'flags', 'last_indexed'))
        for index_id, columns, flags, last_indexed in indexes:
            if flags & self.INDEX_FLAGS.deleting:
                self.indexes_being_removed.append(index_id)
            else:
                if last_indexed < 2**63-1:
                    self.indexes_in_progress.append(columns)
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

    def _next_index_row(self, cursor):
        '''
        Gets the next row that should be indexed.
        '''
        if not self.indexes_in_progress:
            return None, None

        last_indexed = None
        for last_indexed, in cursor.execute('''SELECT MIN(last_indexed) from _indexes'''):
            break

        if last_indexed in (None, 2**63-1):
            return None, None

        query = '''
            SELECT rowid, _id, data, last_updated
                FROM _data
                WHERE last_updated > ?
                ORDER BY last_updated
                LIMIT 1;'''
        for row in cursor.execute(query, (last_indexed,)):
            return last_indexed, row

        self.indexes.update([('last_indexed', 2**63-1)], last_indexed=last_indexed)

        self._refresh_indexes()

        return None, None

    def insert(self, data, cursor=None):
        '''
        Inserts one or more rows into the database, will return a tuple of)
        (uuid, total index rows, number of inserted index rows
        ... for each of the inserted rows.

        All rows will be inserted, or no rows will be inserted.
        '''
        if isinstance(data, list):
            ret = []
            iinsert = []
            for drow in data:
                rowref, row_count, index_rows = _index_rows(drow, self.indexes_to_ids)
                ret.append((rowref, row_count, len(index_rows)))
                iinsert.extend(index_rows)
            with _cursor(cursor or self.db) as cur:
                self.data.insert_many(data, conn=cur)
                self.index.insert_many(iinsert, conn=cur)
            return ret

        rowref, row_count, index_rows = _index_rows(data, self.indexes_to_ids)

        # insert the data, then insert the index rows
        with _cursor(cursor or self.db) as cur:
            self.data.insert(data, conn=cur)
            self.index.insert_many(index_rows, conn=cur)

        return rowref, row_count, len(index_rows)

    def delete(self, id, cursor=None):
        '''
        Deletes one or more rows from the database by uuid.

        Either all rows or no rows will be deleted.
        '''
        if isinstance(id, list):
            with _cursor(cursor or self.db) as cur:
                return map(self.delete, id, itertools.repeat(cur, len(id)))

        with _cursor(cursor or self.db) as cur:
            self.data.delete(_id=id, conn=cur)
            self.index.delete(rowref=id, conn=cur)

    def update(self, data, cursor=None, index_only=False):
        '''
        Updates row or rows provided.  All are updated, or none are updated.
        '''
        if isinstance(data, list):
            with _cursor(cursor or self.db) as cur:
                return map(self.update, itertools.izip(data, itertools.repeat(cur)))

        data = dict(data)
        rowref = data.pop('_id')
        existing_keys = dict((str(k),v) for k,v in self.index.select(('idata', 'rowid'), rowref=rowref))
        old_keys = set(existing_keys)
        indexes = self.indexes_to_ids
        if index_only:
            indexes = dict((index, self.indexes_to_ids[index]) for index in self.indexes_in_progress)
        count, new_keys = generate_index_rows(data, indexes)
        new_keys = set(new_keys)
        to_add = new_keys - old_keys

        with _cursor(cursor or self.db) as cur:
            if not index_only:
                self.data.update(data, rowref, conn=cur)
                to_remove = old_keys - new_keys
                if to_remove:
                    self.index.delete(rowid=sorted(existing_keys[key] for key in to_remove), conn=cur)
            if to_add:
                self.index.insert_many(zip(to_add, itertools.repeat(rowref)), conn=cur)

    def get(self, id, cursor=None):
        if isinstance(id, list):
            with _cursor(cursor or self.db) as cur:
                return map(self.get, id, itertools.repeat(cur, len(id)))

        with _cursor(cursor or self.db) as cur:
            r = self.data.select_one(('data',), _id=id)
            if r:
                r = r[0]
                r['_id'] = id
                return r

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
        self.indexes.insert((index_id, index_def, 0, 0.0), "OR ROLLBACK")

        self._refresh_indexes()

    def drop_index(self, *columns):
        index_def = self._col_def(columns)

        row = self.indexes.select_one(('index_id',), columns=index_def)
        if row:
            index_id, = row
            self.indexes.update([('flags', self.INDEX_FLAGS.deleting)], index_id=index_id)

        self._refresh_indexes()

    def get_drop_key(self):
        self.drop_key = new_uuid()
        return self.drop_key

    def drop_table(self, key):
        if key == self.drop_key:
            self.db.close()
            for i in xrange(10):
                try:
                    os.remove(self.dbfile)
                except errors:
                    time.sleep(.1)
                else:
                    break
            return True
        else:
            return False

    def search(self, filters, order=(), limit=None):
        # todo: check and set limit clause
        query, args = self._gen_query_sql(filters, order, limit)
        out = list(self.db.execute(query, args))
        for i, (data, id) in enumerate(out):
            out[i] = data
            data['_id'] = id
        return out

    def count(self, filters, order=(), limit=None):
        query, args = self._gen_query_sql(filters, order, limit)
        sel, fro, rest = query.partition(' FROM ')
        rest, order, clause = rest.partition(' ORDER BY ')
        query = 'SELECT count(*) FROM ' + rest + ');'
        if limit:
            # want to count the items at an offset or up to a specific limit
            order, lim, lclause = clause.partition(' LIMIT ')
            query += lim + lclause
        for count, in self.db.execute(query, args):
            return count
        return None

    def _gen_query_sql(self, filters, order, limit=None):
        cols = filter_prefix(filters).count(',')
        prefix = cols * [None]
        ok_mini = ['>=', '>']
        ok_maxi = ['<=', '<']
        in_query = False
        neq_query = False
        # mini and maxi will have a shared prefix of data, with an optional
        # minimum and maximum value with comparisons.
        index = -1

        # generate the prefix for our queries
        lc = None
        for col, comparison, value in filters:
            if lc != col:
                index += 1
            lc = col
            if comparison == 'IN':
                in_query = True
                # value is a tuple
                prefix[index] = value
            elif comparison == '=':
                if prefix[index] is not None:
                    raise Exception("bad filter queries")
                prefix[index] = value
            elif comparison in ('<=', '<'):
                neq_query = True
                if prefix[index] is not None:
                    prefix[index][1] = value
                else:
                    prefix[index] = [None, value]
                del ok_maxi[:ok_maxi.index(comparison)]
            elif comparison in ('>=', '>'):
                neq_query = True
                if prefix[index] is not None:
                    prefix[index][0] = value
                else:
                    prefix[index] = [value, Some]
                del ok_mini[:ok_mini.index(comparison)]

        assert None not in prefix
        if in_query + neq_query == 2:
            raise Exception("bad filter queries")

        # find an index/order
        usable_indexes = []
        for prefix_regexp in filter_prefixes(filters, order):
            usable_indexes.append([index for index in self.known_indexes if prefix_regexp.match(index)])

        try:
            use_index = sorted(sum(usable_indexes, []), key=lambda i:i.count(','))[0]
        except IndexError:
            raise TableIndexError("no known indexes match specified query")
        reverse = use_index not in usable_indexes[0]

        # If there exists a minimal index to do what we want (in terms of
        # fewest columns), we will have found it.

        # create the data prefix for our query
        for col_i, (column, value) in enumerate(zip(use_index.split(','), prefix)):
            col_neg = column.startswith('-')
            cased = not column.endswith('-')
            prefix[col_i] = pack(value, case_sensitive=cased, neg=col_neg)

        # inject the index id
        index_id = self.indexes_to_ids[use_index]
        prefix.insert(0, pack(index_id)[1:])

        suffix = None
        if not isinstance(prefix[-1], str):
            suffix = prefix.pop()
        like = ''.join(prefix)
        _i = '_index'
        _t = '_data'
        query = '''
            SELECT %(_t)s.data, %(_t)s._id
                FROM %(_t)s
                INNER JOIN (
                    SELECT DISTINCT %(_i)s.rowref _id
                    FROM %(_i)s
                    WHERE ''' % locals()
        if suffix is None:
            suffix = [None, None]
        if isinstance(suffix, list):
            args = []
            # We would use LIKE here (for prefix equalities), but LIKE may not
            # use indexes, at least for 2.8.6, no idea for the 3 series:
            # http://web.utk.edu/~jplyon/sqlite/SQLite_optimization_FAQ.html
            # We're going to convert LIKE into a pair of comparisons, which
            # should keep things fast, regardless.

            # Do the > or >= part...
            query += '''%s.idata %s ? ''' % (_i, ok_mini[0])
            args.append(like)
            if suffix[0] != None:
                args[-1] += suffix[0]
            query += ''' AND '''

            # Do the < or <= part...
            if suffix[1] == None:
                ok_maxi.pop(0)
                like = _add_one(like)
                args.append(like)
            else:
                args.append(like + suffix[1])
            query += '''%s.idata %s ? ''' % (_i, ok_maxi[0])

        elif isinstance(suffix, tuple):
            # IN queries
            args = []
            query += '''%s.idata IN ? ''' % _i
            for d in suffix:
                args.append(like + d)
            args = [tuple(args)]

        # handle order by clause
        query += ''' ORDER BY %s.idata %s'''% (_i, 'DESC' if reverse else '')

        # and offset/limits
        if limit:
            if isinstance(limit, tuple):
                query += ''' LIMIT %i,%i'''%limit
            else:
                query += ''' LIMIT %i'''%(limit,)
        query += ') SUB ON %(_t)s._id = SUB._id;' % locals()

        # clean up the spacing and return
        return ' '.join(query.split()), tuple(map(buffer, args))
