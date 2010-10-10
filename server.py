
import asyncore
import imp
import itertools
import optparse
import os
import Queue
import signal
import sys
import weakref

from embedded import Database
from lib import adapt
from lib.conf import load_settings, AttrDict
from lib import default_config
from thirdparty.recipe_440665_1 import RequestHandler, Server

IDS = itertools.count()
HANDLER = None
SERVER = None

class DatabaseHandler(Database):
    def __init__(self, config):
        Database.__init__(self, config)
        self._socket_ids = weakref.WeakValueDictionary()
        self._pending_responses = 0

    def _execute(self, sock, rid, table_name, operation, args, kwargs):
        '''
        Execute the provided command on the given table named table_name.

        This function ensures that the table queue_processor() is running, and
        that the request has been sent to the queue processor.
        '''
        # set up all of the necessary processors/queues
        self._socket_ids[sock.id] = sock
        outgoing = self._get_or_setup_command_queue(table_name)
        outgoing.put((sock.id, rid, operation, args, kwargs))
        self._pending_responses += 1

    def _route_responses(self):
        # only route what we've seen so far
        for _i in xrange(self._incoming_responses.qsize()):
            # get a response
            sid, rid, response = self._incoming_responses.get()
            if sid:
                self._pending_responses -= 1
            sock = self._socket_ids.get(sid)
            if sock:
                sock.respond(rid, response)
            del sock

class YogaHandler(RequestHandler):
    server_version = "YogaTable/.9"
    waiting = 1
    def __init__(self, *args, **kwargs):
        RequestHandler.__init__(self, *args, **kwargs)
        self.id = IDS.next()
        self.rid = None
    def _send_resp(self, code, content=None):
        self.send_response(code)
        content = content or ''
        self.send_header("Content-type", 'text/html')
        self.send_header("Content-Length", len(content))
        self.end_headers()
        self.outgoing.append(content)
        self.outgoing.append(None)
        self.handle_write() # to pre-send some data

    def handle_data(self):
        method = self.path.strip('/')
        if 'rid' not in self.body or 'args' not in self.body or 'table' not in self.body:
            return self._send_resp(404)
        rid = self.body['rid'][0]
        if self.rid is not None:
            return self._send_resp(404)
        self.rid = rid
        table = self.body['table'][0]
        args = self.body['args'][0]
        try:
            args, kwargs = adapt.json_converter(args)
        except Exception as e:
            return self._send_resp(404, "bad args")
        HANDLER._execute(self, rid, table, method, args, kwargs)
        self.waiting = 0

    def respond(self, rid, response):
        if rid != self.rid:
            return self._send_resp(404)
        response['rid'] = rid
        self._send_resp(200, adapt.json_adapter(response))

def sigterm_handler(signal, frame):
    print >>sys.stderr, "YogaTable shutting down..."
    global HANDLER, SERVER
    if SERVER:
        SERVER.close()
    if HANDLER:
        # tell the processors to close when done
        HANDLER.shutdown_when_done(False)
    for sock in asyncore.socket_map.values():
        # stop any requests that haven't finished coming in yet
        if sock.waiting:
            sock.close()

def main():
    global HANDLER, SERVER

    parser = optparse.OptionParser()
    parser.add_option('-o', '--host', dest='host',
        help='What interface to listen on, overrides config file (defaults to 127.0.0.1')
    parser.add_option('-p', '--port', dest='port',
        help='What port to listen on, overrides config file', type='int')
    parser.add_option('-c', '--config', dest='config',
        help='What module contains your settings')

    options, args = parser.parse_args()
    if args:
        print "Unexpected arguments passed: %r"%(args,)
        sys.exit(1)
    config = load_settings(getattr(options, 'config', None))
    extra = {}
    if options.host is not None:
        extra['HOST'] = options.host
    if options.port is not None:
        extra['PORT'] = options.port
    if extra:
        config.add(AttrDict(extra))

    # handle the shutdown signal handler
    signal.signal(signal.SIGTERM, sigterm_handler)

    _asyncore = asyncore
    HANDLER = handler = DatabaseHandler(config)
    SERVER = Server(config.HOST, config.PORT, YogaHandler)
    print >>sys.stderr, YogaHandler.__name__, "running on %s:%i"%(config.HOST, config.PORT)
    while _asyncore.socket_map:
        timeout = .01 if handler._pending_responses else 1.0
        _asyncore.poll(timeout=timeout)
        if handler._pending_responses:
            handler._route_responses()

if __name__ == '__main__':
    main()
