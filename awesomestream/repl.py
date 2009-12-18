import code
import sys

from awesomestream.jsonrpc import Client

def main():
    try:
        host = sys.argv[1]
    except IndexError:
        host = 'http://localhost:9997/'
    banner = """>>> from awesomestream.jsonrpc import Client
>>> c = Client('%s')""" % (host,)
    c = Client(host)
    code.interact(banner, local={'Client': Client, 'c': c})

if __name__ == '__main__':
    main()