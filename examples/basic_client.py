import os
import sys

# Munge the path a bit to make this work from directly within the examples dir
sys.path.insert(0, os.path.abspath(os.path.join(__file__, '..', '..')))

from awesomestream.jsonrpc import Client
from pprint import pprint

if __name__ == '__main__':
    c = Client('http://127.0.0.1:9997/')
    items = [
        {'kind': 'play', 'user': 1, 'game': 'bloons'},
        {'kind': 'play', 'user': 1, 'game': 'ryokan'},
        {'kind': 'high-score', 'user': 1, 'game': 'ryokan', 'score': 10},
        {'kind': 'high-score', 'user': 2, 'game': 'ryokan', 'score': 20},
    ]
    # First we insert some data
    for item in items:
        c.insert(item)
    # Now we query it back
    print ">>> c.items()"
    pprint(c.items())
    print ">>> c.items(kind='play')"
    pprint(c.items(kind='play'))
    print ">>> c.items(user=1)"
    pprint(c.items(user=1))
    print ">>> c.items(user=2)"
    pprint(c.items(user=2))
    print ">>> c.items(user=1, kind='play')"
    pprint(c.items(user=1, kind='play'))
    print ">>> c.items(user=1, kind='high-score')"
    pprint(c.items(user=1, kind='high-score'))
    print ">>> c.items(user=[1, 2], kind='high-score')"
    pprint(c.items(user=[1, 2], kind='high-score'))
    print ">>> c.items(user=[1, 2], kind=['high-score', 'play'])"
    pprint(c.items(user=[1, 2], kind=['high-score', 'play']))
