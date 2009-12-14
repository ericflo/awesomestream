import os
import sys

# Munge the path a bit to make this work from directly within the examples dir
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))

from awesomestream.backends import RedisBackend
from awesomestream.jsonrpc import create_app, run_server

if __name__ == '__main__':
    backend = RedisBackend(
        keys=['kind', 'user', 'game'],
        host='127.0.0.1',
        port=6379
    )
    app = create_app(backend)
    run_server(app, 8080)