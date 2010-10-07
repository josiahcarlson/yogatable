
#-----------------------------------------------------------------------------
# YogaTable specific options
#-----------------------------------------------------------------------------
# Where to store the data for YogaTable.
PATH = '.'
# What port to listen on when using the server version of YogaTable
PORT = 8765
# What host to listen on when using the server version of YogaTable
HOST = '127.0.0.1'
# How long to allow the embedded/remote server to lag while indexing data, 
# unindexing data, or vacuuming the underlying database.  The higher the
# number here, the faster the maintenance operations will finish.  The lower
# the number here, the lower the latency when a request does finally come in.
DESIRED_LATENCY = .010
# How long to wait until starting to perform maintence operations after
# responding to queries.
IDLE_TIMEOUT = .025
# How many responses to process per attempt to clean up old thread queues.
THREAD_CLEANUP_RATE = 256

# Note: the MAX_INDEX_ROW_COUNT will be checked before discarding of rows as
# part of MAX_INDEX_ROW_LENGTH/ROW_TOO_LONG='discard'.

# The maximum number of index rows to produce per row inserted.
MAX_INDEX_ROW_COUNT = 100
# What to do if too many index rows are created for a data row: 'fail' will
# raise an exception, 'discard' will ignore extra index rows.  Which rows to
# be ignored when 'discard' is provided is not determined.
TOO_MANY_ROWS = 'fail'
# The maximum length of the packed representation of the indexed columns.
MAX_INDEX_ROW_LENGTH = 512
# What to do if a packed index row is too long: 'fail' will raise an
# exception, 'discard' will discard any such rows, 'truncate' will trim the
# row itself.
ROW_TOO_LONG = 'fail'

# When using the embedded or server modules, what tables to automatically
# start processors for at startup?
# Used as:
# AUTOLOAD_TABLES = ['table1', 'table2', ...]
AUTOLOAD_TABLES = []

# When you want non-standard configuration for your tables, you can override
# the table configuration like:
# TABLE_CONFIGURATION = {'table1': {'PATH':'/var/local/table1'}}
TABLE_CONFIGURATION = {}


# Note: try to keep MINIMUM_VACUUM_BLOCKS values reasonably low, it will
# keep YogaTable responsive, even during cleanup.

# If AUTOVACUUM is set to 2, what is the minimum number of blocks that must be
# empty to cause an incremental vacuum after indexing/unindexing?
MINIMUM_VACUUM_BLOCKS = 100


#-----------------------------------------------------------------------------
# The underlying sqlite configuration options.
#-----------------------------------------------------------------------------
# autovacuum status, use 1 or 2 as per sqlite docs:
# http://www.sqlite.org/pragma.html#pragma_auto_vacuum
# Note: when changing this for an existing table from 0 to 1/2 or from 1/2 to
# 0, YogaTable will automatically vacuum the underlying SQLite database.
# Don't change this for an existing table unless you know what you are doing.
# Also, as per http://www.sqlite.org/releaselog/3_7_2.html , using autovacuum
# 2 may result in corruption, depending on your version of sqlite.
AUTOVACUUM = 1
# Number of blocks to tell the system to cache per table, increase or decrease
# as your needs warrant.  Generally, if you have memory to spare, this will
# improve performance significantly.
CACHE_SIZE = 2000
# size of blocks in newly created tables
BLOCK_SIZE = 8192
