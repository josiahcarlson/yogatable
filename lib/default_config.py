
#-----------------------------------------------------------------------------
# YogaTable specific options
#-----------------------------------------------------------------------------
# Where to store the data for YogaTable.
PATH = '.'
# How long to allow the embedded/remote server to lag while indexing or
# unindexing data.
DESIRED_LATENCY = .01
# How many idle passes through the processor loop to perform before attempting
# to index/unindex data.
ALLOWED_IDLE_PASSES = 1
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

# Note: try to keep the below *_VACUUM_BLOCKS values reasonably low, it will
# keep YogaTable responsive, even during cleanup.

# If AUTOVACUUM is set to 2, what is the minimum number of blocks that must be
# empty to cause an incremental vacuum after ALLOWED_IDLE_PASSES have passed?
MINIMUM_VACUUM_BLOCKS = 100
# If AUTOVACUUM is set to 2, what is the maximum number of blocks that will be
# reclaimed in a single incremental vacuum pass?
MAXIMUM_VACUUM_BLOCKS = 1000



#-----------------------------------------------------------------------------
# The underlying sqlite configuration options.
#-----------------------------------------------------------------------------
# autovacuum status, use 1 or 2 as per sqlite docs:
# http://www.sqlite.org/pragma.html#pragma_auto_vacuum
# Note: when changing this for an existing table from 0 to 1/2 or from 1/2 to
# 0, YogaTable will automatically vacuum the underlying SQLite database.
# Don't change this for an existing table unless you know what you are doing.
AUTOVACUUM = 1
# number of blocks to tell the system to cache per table
CACHE_SIZE = 2000
# size of blocks in newly created tables
BLOCK_SIZE = 8192
