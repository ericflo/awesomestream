import sys
import traceback

from uuid import uuid1

from urlparse import urlparse
from httplib import HTTPConnection

from simplejson import dumps, loads

from werkzeug import Request, Response
from werkzeug.exceptions import HTTPException, NotFound, BadRequest

def create_app(backend):
    @Request.application
    def application(request):
        try:
            # Parse the JSON in the request
            try:
                data = loads(request.stream.read())
            except ValueError:
                raise BadRequest()
            # Grab the function to execute
            try:
                method = getattr(backend, data['method'])
            except (KeyError, IndexError):
                raise BadRequest()
            if method is None:
                raise NotFound()
            # Get the args and kwargs
            args = data.get('args', [])
            kwargs = data.get('kwargs', {})
            kwargs = dict(((k.encode('utf-8'), v) for k, v in kwargs.iteritems()))
            # Attempt to call the method with the params, or catch the
            # exception and pass that back to the client
            try:
                response = Response(dumps({
                    'id': data.get('id'),
                    'result': method(*args, **kwargs),
                    'error': None,
                }))
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception, e:
                print e
                response = Response(dumps({
                    'id': data.get('id'),
                    'result': None,
                    'error': ''.join(traceback.format_exception(*sys.exc_info())),
                }))
            # Finish up and return the response
            response.headers['Content-Type'] = 'application/json'
            response.headers['Content-Length'] = len(response.data)
            response.status_code = 200
            return response
        except HTTPException, e:
            # If an http exception is caught we can return it as response
            # because those exceptions render standard error messages when
            # called as wsgi application.
            return e
    return application

def run_server(app, port, numthreads=10):
    from cherrypy import wsgiserver
    server = wsgiserver.CherryPyWSGIServer(('0.0.0.0', port), app,
        numthreads=numthreads)
    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()

class Client(object):
    def __init__(self, server):
        self.server = server
        url_parts = urlparse(server)
        self.port = url_parts.port
        self.host, _, _ = url_parts.netloc.partition(':')
        self.headers = {'Content-Type': 'application/json'}

    def __getattr__(self, obj):
        return self._request(obj)

    def _request(self, method):
        def _inner(*args, **kwargs):
            data = dumps({
                'id': str(uuid1()),
                'method': method,
                'args': args,
                'kwargs': kwargs,
            })
            conn = HTTPConnection(self.host, self.port)
            conn.request('POST', '/', data, self.headers)
            response = conn.getresponse().read()
            decoded_response = loads(response)
            conn.close()
            error = decoded_response.get('error')
            if error is not None:
                raise ValueError(error)
            return decoded_response.get('result')
        return _inner