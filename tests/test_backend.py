import unittest

from awesomestream.backends import MemoryBackend

class MemoryBackendTest(unittest.TestCase):
    def setUp(self):
        self.backend = MemoryBackend(keys=['kind', 'user', 'game'])
    
    def test_basic(self):
        items = [
            {'kind': 'play', 'user': 1, 'game': 'bloons'},
            {'kind': 'play', 'user': 1, 'game': 'ryokan'},
            {'kind': 'high-score', 'user': 1, 'game': 'ryokan', 'score': 10},
            {'kind': 'high-score', 'user': 2, 'game': 'ryokan', 'score': 20},
        ]
        # First we insert some data
        for item in items:
            self.backend.insert(item)
        # Now we assert that it comes back properly in different queries
        self.assertEqual(self.backend.items(), list(reversed(items)))
        self.assertEqual(self.backend.items(kind='play'), [items[1], items[0]])
        self.assertEqual(self.backend.items(user=1),
            [items[2], items[1], items[0]])
        self.assertEqual(self.backend.items(user=2), [items[3]])
        self.assertEqual(self.backend.items(user=1, kind='play'),
            [items[1], items[0]])
        self.assertEqual(self.backend.items(user=1, kind='high-score'),
            [items[2]])
        self.assertEqual(self.backend.items(user=[1, 2]), list(reversed(items)))

if __name__ == '__main__':
    unittest.main()