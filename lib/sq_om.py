
from functools import wraps
import time

# imported for the side-effect
from .lib import sq_adapt
sq_adapt # to silence the pyflakes warning

def _check_data(method):
    @wraps(method)
    def call(self, data, *args, **kwargs):
        if isinstance(data, dict):
            data = [data[col] for col in self._cols]
        assert len(data) == len(self._cols), (data, self._cols)
        return method(self, data, *args, **kwargs)
    return call

def _check_data_seq(method):
    @wraps(method)
    def call(self, data_seq, *args, **kwargs):
        for i, data in enumerate(data_seq):
            if isinstance(data, dict):
                data = [data[col] for col in self._cols]
                data_seq[i] = data
            assert len(data) == len(self._cols), (data, self._cols)
        return method(self, data_seq, *args, **kwargs)
    return call

def _where_clause(table_name, cols, where, kwhere, select=True):
    assert bool(where) ^ bool(kwhere) < 2, "use a list OR keywords, not both: %r %r"%(where, kwhere)
    if not where:
        where = kwhere
    if isinstance(where, dict):
        where = where.items()
    where_clause = ''
    vals = ()
    if where:
        colnames, vals = zip(*where)
        compare = dict((col, '=' if isinstance(val, (list, tuple)) else '=') for col, val in where)
        # This str(tuple(val)) shenanigans is because Python's sqlite3
        # library doesn't know how to properly pass a tuple down through the
        # layers for a proper 'IN' query.  :(
        vals = tuple(str(tuple(val)) if isinstance(val, (list, tuple)) else val for val in vals)
        where_clause = ' WHERE ' + ' AND '.join('%s %s ?'%(col, compare[col]) for col in colnames)
    if select:
        query = '''SELECT %s FROM %s %s;'''%(
            ', '.join(cols), table_name, where_clause)
    else:
        query = '''DELETE FROM %s %s;'''%(
            table_name, where_clause)
    return vals, query

class SQLTable(object):
    indexes = ()
    def __init__(self, db):
        self.db = db
        self.setup()
    def setup(self):
        self._cols = [colname.partition(' ')[0] for colname in self.columns]
        columns = [colname + ' NOT NULL' if 'NOT NULL' not in colname else colname for colname in self.columns]
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS
                %s (%s);
            '''%(self.table_name, ', '.join(columns)))
        for name, cols, unique in self.indexes:
            self.db.execute('''
            CREATE %s INDEX IF NOT EXISTS
                %s ON %s (%s);
            '''%('UNIQUE' if unique else '', name, self.table_name, ', '.join(cols)))

    @_check_data
    def insert(self, data, OR='', conn=None):
        conn = conn or self.db
        return conn.execute('''
            INSERT %s INTO %s (%s) VALUES (%s);
            '''%(OR, self.table_name, ', '.join(self._cols), ', '.join(len(self._cols)*['?'])),
            tuple(data)).lastrowid

    @_check_data_seq
    def insert_many(self, data, conn=None):
        conn = conn or self.db
        return conn.executemany('''
            INSERT INTO %s (%s) VALUES (%s);
            '''%(self.table_name, ', '.join(self._cols), ', '.join(len(self._cols)*['?'])),
            data)

    @_check_data
    def update(self, data, uuid, conn=None):
        conn = conn or self.db
        return conn.execute('''
            UPDATE OR ROLLBACK
                %s SET %s
            WHERE _id = ?;
            '''%(self.table_name, ', '.join('%s = ?'%col for col in self._cols)),
            tuple(data) + (uuid,))

    def select_one(self, cols, where=None, conn=None, **kwhere):
        conn = conn or self.db
        vals, query = _where_clause(self.table_name, cols, where, kwhere)
        for result in conn.execute(query, vals):
            return result
        return None

    def select(self, cols, where=None, conn=None, **kwhere):
        conn = conn or self.db
        vals, query = _where_clause(self.table_name, cols, where, kwhere)
        return list(conn.execute(query, vals))

    def delete(self, where=None, conn=None, **kwhere):
        conn = conn or self.db
        vals, query = _where_clause(self.table_name, None, where, kwhere, select=False)
        return conn.execute(query, vals)

class IndexInfo(SQLTable):
    columns = 'index_id INTEGER PRIMARY KEY', 'columns TEXT UNIQUE', 'flags INTEGER', 'last_indexed FLOAT'
    table_name = '_indexes'

class IndexTable(SQLTable):
    columns = 'rowid INTEGER PRIMARY KEY', 'idata BLOB', 'rowref TEXT'
    table_name = '_index'
    indexes = [
        ['_index_idata', ('idata',), False],
        ['_index_irowref', ('rowref',), False],
    ]
    def __init__(self, db):
        SQLTable.__init__(self, db)
        # We don't want to be inserting based on rowid, so we'll pretend it
        # doesn't exist from this side of things.
        self._cols = self._cols[1:]

@apply
def CAN_USE_CLOCK():
    '''
    Some virtualized systems mess up the resolution of time.clock(), we'll use
    it if we can use it, otherwise we'll generate our own time sequence with
    the properties we require.
    '''
    clocks = [time.clock() for i in xrange(10000)]
    return len(clocks) != len(set(clocks))

def _time_seq(_lt=[time.time(), 0], CAN_USE_CLOCK=CAN_USE_CLOCK):
    '''
    We want to generate monotonically increasing time-based sequences, as
    close to reality as possible, modulo system precision, and someone
    changing the clock on the system.

    IEEE 754 FP doubles can only support precision of .238 microseconds with
    the current magnitude of unix time, which gets us around 4 million rows
    per second... hopefully that's enough.

    We use this sequence as a method of defining insertion/update order for
    the rows in our data table, so that indexing operations can merely walk an
    index over the time column to discover those rows that need to be indexed.
    '''
    if CAN_USE_CLOCK:
        # This will get us the best possible real time resolution.  Roughly
        # 1.2 microseconds resolution on a 2.4 ghz core 2 duo.
        now = time.time()
        clk = time.clock()
        while 1:
            yield now + time.clock() - clk
    else:
        # This will give us up to 4096 insertions per millisecond, which
        # should be sufficient for platforms with a shoddy time.clock().
        # This also ends up being the resolution of IEEE 754 FP doubles with
        # the current magnitude of unix time.
        lt = _lt[0]
        i = _lt[1]
        shift = float(2**22)
        while 1:
            t = time.time()
            if t != lt:
                i = 0
                lt = _lt[0] = t
            yield t + i / shift
            i += 1
            _lt[1] = i

class DataTable(IndexTable):
    columns = 'rowid INTEGER PRIMARY KEY', '_id TEXT UNIQUE', 'data JSON', 'last_updated FLOAT KEY'
    indexes = ()
    table_name = '_data'
    def insert(self, data, conn=None):
        return SQLTable.insert(self, (data.pop('_id'), data, time.time()), conn=conn)
    def insert_many(self, data, conn=None):
        return SQLTable.insert_many(self, zip((d.pop('_id') for d in data), data, _time_seq()), conn=conn)
    def update(self, data, uuid, conn=None):
        return SQLTable.update(self, (uuid, data, time.time()), uuid, conn=conn)
