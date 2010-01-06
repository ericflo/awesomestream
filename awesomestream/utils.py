import time
import datetime

def coerce_ts(value=None):
    '''
    Given a variety of inputs, this function will return the proper
    timestamp (a float).  If None or no value is given, then it will
    return the current timestamp.
    '''
    if value is None:
        return time.time()
    if isinstance(value, int):
        value = float(value)
    if isinstance(value, datetime.timedelta):
        value = datetime.datetime.now() + value
    if isinstance(value, datetime.date):
        value = datetime.datetime(year=value.year, month=value.month,
            day=value.day)
    if isinstance(value, datetime.datetime):
        value = float(time.mktime(value.timetuple()) * 1e6)
    return value

def coerce_dt(value=None):
    '''
    Given a variety of inputs, this function will return the proper
    ``datetime.datetime`` instance.  If None or no value is given, then
    it will return ``datetime.datetime.now()``.
    '''
    if value is None:
        return datetime.datetime.now()
    if isinstance(value, int):
        value = float(value)
    if isinstance(value, float):
        return datetime.datetime.fromtimestamp(value)
    if isinstance(value, datetime.date):
        return datetime.datetime(year=value.year, month=value.month,
            day=value.day)
    if isinstance(value, datetime.timedelta):
        value = datetime.datetime.now() + value
    return value

# TODO: The following are not the proper function names. Should figure out
#       exactly what we want to call these so that it's less ambiguous to
#       someone new to the code.

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