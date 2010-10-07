
from collections import deque
from functools import wraps
import os
from Queue import Empty
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

class QueueWrapper(object):
    def __init__(self, q):
        self.q = q
        self.v = deque()
    def get(self, *args, **kwargs):
        if self.v:
            return self.v.popleft()
        return self.q.get(*args, **kwargs)
    def push(self, v):
        self.v.append(v)
    def qsize(self):
        return len(self.v) + self.q.qsize()

def _new_count(old_count, delta, desired, minimum, maximum):
    return int(min(maximum, max(minimum, old_count * desired / (delta or .001))))

@handle_exception
def queue_processor(config, table, queue, results):
    queue = QueueWrapper(queue)
    # open or create the table
    table_adapter = adapt_table.TableAdapter(os.path.join(config.PATH, table + '.sqlite'), table, config)
    # respond with the list of known indexes
    results.put((None, None, {'response':'indexes', 'table_name':table, 'value':table_adapter.known_indexes}))
    idle_sleep = max(.001, min(.1, config.DESIRED_LATENCY))
    keep_running = True
    check_for_idle_work = True
    index_count = 1
    delete_count = 1
    vacuum_count = 1
    while keep_running:
        qsize = queue.qsize()

        # state | check | qsize
        # 1     | *     | >0    -> perform work, check=True
        # 2     | True  | 0     -> wait for work/do idle work, check=False on no work
        # 3     | False | 0     -> wait for work

        # 1 -> (1, 2)
        # 2 -> (1, 2, 3)
        # 3 -> (1, 3)

        # When there are queries to process, it will hang out in state 1.
        # After all queries are handled, it will transition to state 2, where
        # it will perform any idle tasks it is able to do.  If there are items
        # in the queue, it will transition back to state 1.
        # Once completed with all of the idle work, the processor will
        # transition to state 3, where it will remain until there is work to
        # do.

        if check_for_idle_work and not qsize:
            in_progress = table_adapter.indexes_in_progress
            being_deleted = table_adapter.indexes_being_removed

            # If we don't have any pending operations, and we haven't performed
            # any operations for a little while...do some indexing.
            if in_progress:
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
                # balance it.
                index_count = _new_count(index_count, time.time() - now, idle_sleep, 1, 100)

            # If we don't have any pending operations, and we haven't performed
            # any operations for a little while...delete some indexes.
            elif being_deleted:
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
                # delete as long as it stays under our desired latency.
                delete_count = _new_count(delete_count, time.time() - now, idle_sleep, 1, 5000)

            elif config.AUTOVACUUM == 2:
                now = time.time()
                fc = table_adapter._pragma_read('freelist_count')
                if fc >= config.MINIMUM_VACUUM_BLOCKS:
                    table_adapter.db.execute('PRAGMA incremental_vacuum(%i)'%(vacuum_count,))
                    fc -= table_adapter._pragma_read('freelist_count')
                    vacuum_count = _new_count(max(fc, 1), time.time() - now, idle_sleep, 1, 5000)
                else:
                    check_for_idle_work = False
            else:
                check_for_idle_work = False

            continue

        elif not qsize:
            try:
                queue.push(queue.get(timeout=config.IDLE_TIMEOUT))
            except Empty:
                continue

        check_for_idle_work = True

        q = queue.get()
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
