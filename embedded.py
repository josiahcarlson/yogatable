
'''
This module allows the use of YogaTable as an embedded database.  All of the
NoSQL flexibility, without needing to set up a server.
'''

import multiprocessing
import Queue
import re
import threading
import time

from lib import exceptions
from lib import processor

def whoami():
    return threading.currentThread().ident

class Database(object):
    '''
    Instantiate one of me with the path where your tables should be stored.
    '''
    def __init__(self, path, config):
        self._config = config
        self._shutting_down = False
        self._outgoing_queues = {}
        self._processors = {}
        self._incoming_responses = multiprocessing.Queue()
        self._lock = threading.RLock()
        self._responses = {}
        self._response_router = None
        self._known_indexes = {}
        self._local = threading.local()
        self._tables = {}

    def shutdown_when_done(self):
        '''
        Shutdown all processors when they've completed all currently
        outstanding queries.
        '''
        self._shutting_down = True
        sent = set()
        while self._processors:
            with self._lock:
                known = set(self._processors)
                to_kill = known - sent
                for name in to_kill:
                    x = self._outgoing_queues.get(name, None)
                    if x:
                        x.put((None, None, '_quit', (), {}))
                sent = known
            time.sleep(.001)

    def shutdown_with_kill(self):
        '''
        Kill all processor subprocesses.
        '''
        self._shutting_down = True
        while self._processors:
            with self._lock:
                for n,p in self._processors.items():
                    p.terminate()
                    if not p.is_alive():
                        del self._processors[n]
            time.sleep(.001)

    def _cleanup(self):
        '''
        I should be called every once in a while when there is significant
        thread churn.
        '''
        with self._lock:
            clean = set(self._responses) - set(t.ident for t in threading.enumerate())
            for tid in clean:
                del self._responses[tid]
        return len(clean)

    def _get_or_setup_command_queue(self, table_name):
        '''
        Start up a queue_processor process for each table.
        '''
        if self._shutting_down:
            return
        with self._lock:
            if table_name not in self._outgoing_queues:
                self._outgoing_queues[table_name] = multiprocessing.Queue()
            if table_name not in self._processors or not self._processors[table_name].is_alive():
                self._processors[table_name] = p = multiprocessing.Process(
                    target=processor.queue_processor,
                    args=(self._config,
                          table_name,
                          self._outgoing_queues[table_name],
                          self._incoming_responses))
                p.daemon = True
                p.start()
        return self._outgoing_queues[table_name]

    def _get_or_setup_response(self):
        '''
        Create a queue for every thread.  Workloads with large thread churn
        should occasionally call the _cleanup() function above.
        '''
        if self._shutting_down:
            return
        tid = whoami()
        with self._lock:
            if tid not in self._responses:
                self._responses[tid] = Queue.Queue()
        return self._responses[tid]

    def _setup_response_router_if_necessary(self):
        '''
        Rather than having a bunch of threads waiting on passed queues, we'll
        go ahead and create a designated listening thread that will handle the
        routing to the proper result queue.
        '''
        if self._shutting_down:
            return
        with self._lock:
            if self._response_router is None:
                self._response_router = threading.Thread(target=self._route_responses)
                self._response_router.setDaemon(1)
                self._response_router.start()

    def _route_responses(self):
        '''
        This method is run as a thread, and routes the responses from the
        queue_processor() processes to the requesting thread's queue.
        '''
        passes = 0
        Empty = Queue.Empty
        while (not self._shutting_down) or self._processors:
            passes += 1
            if not passes % (self._config.THREAD_CLEANUP_RATE * (len(self._responses) or 1)):
                # handle thread cleanup every once in a while
                self._cleanup()
            # get a response
            try:
                response = self._incoming_responses.get(timeout=1)
            except Empty:
                continue
            # find it's destination
            rqueue = self._responses.get(response[0])
            if rqueue:
                # We'll only bother to route messages to destinations that
                # currently exist.
                rqueue.put(response)
            elif response[0] == response[1] == None:
                response = response[2]
                resp = response.get('response')
                table_name = response['table_name']
                if resp == 'indexes':
                    # handle index updates
                    self._known_indexes[table_name] = response['value']
                elif resp == 'quit':
                    self._processors.pop(table_name)
        self._response_router = None

    def _execute(self, table_name, operation, args, kwargs):
        '''
        Execute the provided command on the given table named table_name.

        This function ensures that the table queue_processor() is running, that
        there exists a queue for this thread to wait on, and that the thread that
        routes responses is running, and finally, it waits for the response
        itself.
        '''
        # set up all of the necessary processors/queues
        outgoing = self._get_or_setup_command_queue(table_name)
        incoming = self._get_or_setup_response()
        self._setup_response_router_if_necessary()
        # get a counter so that we know which command is being executed
        if not hasattr(self._local, 'counter'):
            self._local.counter = 0
        self._local.counter += 1
        # get the processing started
        outgoing.put((whoami(), self._local.counter, operation, args, kwargs))
        while 1:
            # wait for the response
            me, id, response = incoming.get()
            if id != self._local.counter:
                # ignore responses not directed at me
                continue
            break

        exceptions.check_response(response)
        assert response['response'] == 'ok'
        return response['value']

    # you can access tables by db['name'] or by db.name
    def __getattr__(self, attr):
        return self[attr]

    def __getitem__(self, table):
        if table not in self._tables:
            self._tables[table] = Table(self, table)
        return self._tables[table]

class Table(object):
    '''
    Don't instantiate me directly, let a Database instance create me.
    '''
    def __init__(self, db, table):
        if not re.match('^[_a-z0-9]+$', table):
            raise exceptions.TableNameError("Table %r does not match the table name regular expression [_a-z0-9]+", table)
        self.db = db
        self.table = table
    @property
    def known_indexes(self):
        return self.db._known_indexes.get(self.table, [])
    def __getattr__(self, attr):
        return Operation(self.db, self.table, attr)

class Operation(object):
    '''
    Don't instantiate me directly, let a Table instance create me.
    '''
    def __init__(self, db, table, name):
        self.db = db
        self.table = table
        self.operation = name
    def __call__(self, *args, **kwargs):
        return self.db._execute(self.table, self.operation, args, kwargs)
