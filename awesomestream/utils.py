import time
import datetime

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

def coerce_ts(value):
    if value is None:
        value = int(time.time() * 1e6)
    if isinstance(value, float):
        value = int(value * 1e6)
    if isinstance(value, datetime.timedelta):
        value = datetime.datetime.now() + value
    if isinstance(value, datetime.date):
        value = datetime.datetime(year=value.year, month=value.month,
            day=value.day)
    if isinstance(value, datetime.datetime):
        value = int(time.mktime(value.timetuple()) * 1e6)
    return value

def coerce_dt(value):
    if value is None:
        value = datetime.datetime.now()
    if isinstance(value, (float, int)):
        value = value / float(1e6)
    if isinstance(value, float):
        value = datetime.fromtimestamp(value)
    if isinstance(value, datetime.date):
        value = datetime.datetime(year=value.year, month=value.month,
            day=value.day)
    if isinstance(value, datetime.timedelta):
        value = datetime.datetime.now() + value
    return value