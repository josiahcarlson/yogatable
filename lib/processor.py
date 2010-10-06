
from functools import wraps
import os
import time
import traceback

from .lib import exceptions
from .lib import pack
from .lib import table as adapt_table

def handle_exception(processor):
    @wraps(processor)
    def run(config, table, queue, results):
        try:
            return processor(config, table, queue, results)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            out = traceback.format_exc().rstrip()
            results.put((None, None, {'exception':'UnknownExceptionError', 'table_name':table, 'args':(out,)}))
            results.put((None, None, {'response':'quit', 'table_name':table}))
            raise
    return run

@handle_exception
def queue_processor(config, table, queue, results):
    # open or create the table
    table_adapter = adapt_table.TableAdapter(os.path.join(config.PATH, table + '.sqlite'), table, config)
    # respond with the list of known indexes
    results.put((None, None, {'response':'indexes', 'table_name':table, 'value':table_adapter.known_indexes}))
    idle_sleep = max(.001, min(.1, config.DESIRED_LATENCY))
    keep_running = True
    idle_passes = 0
    index_count = 1
    delete_count = 1
    while keep_running:
        qsize = queue.qsize()
        in_progress = table_adapter.indexes_in_progress
        being_deleted = table_adapter.indexes_being_removed

        # If we don't have any pending operations, and we haven't performed
        # any operations for a little while...do some indexing.
        if in_progress and not qsize and idle_passes > config.ALLOWED_IDLE_PASSES:
            now = time.time()
            with table_adapter.db as cursor:
                li, rows = table_adapter._next_index_row(index_count, cursor)
                if not rows:
                    # just finished catching up with indexes
                    continue
                last_updated = li
                for row in rows:
                    rowid, _id, data, last_updated = row
                    data['_id'] = _id
                    table_adapter.update(data, cursor, index_only=True)
                table_adapter.indexes.update([('last_indexed', last_updated)], last_indexed=li, conn=cursor)
            # Ultimately, we want to increase the number of rows we index at a
            # time in order to increase indexing performance.  However, that
            # also increases the latency for subsequent queries, so we need to
            # balance it.  We'll say that an expected 100ms latency is
            # acceptable during continuous indexing, and we'll also wait the
            # same 100 ms after queries have come in before we start indexing
            # again.
            dt = (time.time() - now) or .001
            index_count = int(min(100, max(1, index_count * idle_sleep / dt)))
            idle_passes += 1
            continue

        # If we don't have any pending operations, and we haven't performed
        # any operations for a little while...delete some indexes.
        if being_deleted and not qsize and idle_passes > config.ALLOWED_IDLE_PASSES:
            now = time.time()
            to_delete = being_deleted[0]
            start = pack.pack(to_delete)[1:]
            end = pack.pack(to_delete+1)[1:]
            deleted = table_adapter.index.delete_some(start, end, delete_count)
            if not deleted:
                being_deleted.pop(0)
                table_adapter.indexes.delete(index_id=to_delete)
                table_adapter._refresh_indexes()
                results.put((None, None, {'response':'indexes', 'table_name':table, 'value':table_adapter.known_indexes}))
                deleted += 1
            # A similar argument applies to row deletion as we made for row
            # indexing.  We'll increase the number of index rows that we'll
            # delete as long as it stays under our desired 100ms latency.
            dt = (time.time() - now) or .001
            delete_count = int(min(5000, max(1, deleted * idle_sleep / dt)))
            idle_passes += 1
            continue

        if not qsize and config.AUTOVACUUM == 2 and idle_passes > config.ALLOWED_IDLE_PASSES:
            fc = table_adapter._pragma_read('freelist_count')
            if fc >= config.MINIMUM_VACUUM_BLOCKS:
                vac = min(fc, config.MAXIMUM_VACUUM_BLOCKS)
                rem = fc - vac
                if rem and rem < config.MINIMUM_VACUUM_BLOCKS:
                    # If we were to vacuum the maximum, then we couldn't
                    # vacuum the remainder next pass.
                    vac -= config.MINIMUM_VACUUM_BLOCKS
                table_adapter.db.execute('PRAGMA incremental_vacuum(%i)'%(vac,))
            continue

        if not qsize:
            # sleep if we have no work to do
            idle_passes += 1
            time.sleep(.001)
            continue

        idle_passes = 0

        q = queue.get()
        if len(q) != 5:
            print "wha?", q
        sid, oid, operation, args, kwargs = q
        if sid is None:
            break

        old = None
        try:
            # perform the operation
            if isinstance(operation, str) and operation[:1] != '_' and hasattr(table_adapter, operation):
                if '_index' in operation:
                    old = set(table_adapter.known_indexes)
                response = {'response':'ok', 'value':getattr(table_adapter, operation)(*args, **kwargs)}
            elif operation == '_quit':
                results.put((None, None, {'response':'quit', 'table_name':table}))
                break
            else:
                raise exceptions.InvalidOperation(operation)
        except Exception as e:
            response = {'exception':e.__class__.__name__, 'table_name':table, 'args':e.args}
        else:
            # handle index changes
            if old is not None:
                new = set(table_adapter.known_indexes)
                if new != old:
                    results.put((None, None, {'response':'indexes', 'table_name':table, 'value':table_adapter.known_indexes}))
            # drop the table, so drop the processor for the table
            if operation == 'drop_table' and response['value']:
                keep_running = False
                results.put((None, None, {'response':'indexes', 'table_name':table, 'value':[]}))
                results.put((None, None, {'response':'quit', 'table_name':table}))
        results.put((sid, oid, response))
