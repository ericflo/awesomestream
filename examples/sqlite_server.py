import os
import sys

# Munge the path a bit to make this work from directly within the examples dir
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))

from awesomestream.backends import SQLBackend
from awesomestream.jsonrpc import create_app, run_server

if __name__ == '__main__':
    backend = SQLBackend(
        dsn='sqlite:////tmp/stream.db',
        keys=['kind', 'user', 'game'],
    )
    app = create_app(backend)
    run_server(app, 8080)