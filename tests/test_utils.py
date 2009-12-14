import unittest

from awesomestream.utils import permutations

class PermutationsTest(unittest.TestCase):
    def test_multiple_permutations(self):
        vals = [[1, 2, 3], [4, 5], [6, 7]]
        self.assertEqual(
            list(permutations(vals)),
            [
                [1, 4, 6],
                [2, 4, 6],
                [3, 4, 6],
                [1, 5, 6],
                [2, 5, 6],
                [3, 5, 6],
                [1, 4, 7],
                [2, 4, 7],
                [3, 4, 7],
                [1, 5, 7],
                [2, 5, 7],
                [3, 5, 7],
            ]
        )
    
    def test_single_permutation(self):
        vals = [['asdf']]
        self.assertEqual(list(permutations(vals)), [['asdf']])
    
    def test_double_permutation(self):
        vals = [['1', '2']]
        self.assertEqual(list(permutations(vals)), [['1'], ['2']])