def combinations(iterable, r):
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = range(r)
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)

def all_combinations(iterable):
    for r in range(1, len(iterable) + 1):
        for item in combinations(iterable, r):
            yield item

def permutations(lst):
    current = [-1] + ([0] * (len(lst) - 1))
    maxes = [len(item) - 1 for item in lst]
    while 1:
        for i, (c, m) in enumerate(zip(current, maxes)):
            if i > 0:
                current[i - 1] = 0
            if c < m:
                current[i] = c + 1
                break
        yield [lst[i][idx] for i, idx in enumerate(current)]
        if current == maxes:
            raise StopIteration
