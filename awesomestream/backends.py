import collections
import datetime
import simplejson
import uuid

from awesomestream.utils import all_combinations, permutations
from awesomestream.utils import coerce_ts, coerce_dt

class BaseBackend(object):
    def __init__(self, keys=None):
        self.keys = keys or []
    
    def insert(self, data, date=None):
        raise NotImplementedError
    
    def keys_from_keydict(self, keydict):
        if not keydict:
            yield '_all'
            raise StopIteration
        idx_key_parts = sorted(keydict.keys())
        # Make sure all values are lists
        values = []
        for k in idx_key_parts:
            value = keydict[k]
            if not isinstance(value, (list, tuple)):
                value = [value]
            values.append([unicode(v).encode('utf-8') for v in value])
        for value_permutation in permutations(values):
            yield '-'.join(idx_key_parts + value_permutation)
    
    def indexes_from_data(self, data):
        # Maintain all of the other indices
        for key_list in all_combinations(self.keys):
            add_key, keydict = True, {}
            for k in key_list:
                value = data.get(k)
                if value is None:
                    add_key = False
                    break
                else:
                    keydict[k] = value
            if add_key:
                for k in self.keys_from_keydict(keydict):
                    yield k
    
    def serialize(self, data):
        if data is None:
            return None
        return simplejson.dumps(data)
    
    def deserialize(self, data):
        if data is None:
            return None
        return simplejson.loads(data)


class MemoryBackend(BaseBackend):
    def __init__(self, keys=None):
        super(MemoryBackend, self).__init__(keys=keys)
        self._items = {}
        self._indices = collections.defaultdict(lambda: [])
    
    def insert(self, data, date=None):
        key = str(uuid.uuid1())
        
        self._items[key] = self.serialize(data)
        
        t = coerce_ts(date)
        
        # Insert into the global index
        self._indices['_all'].insert(0, (t, key))
        
        # Maintain the other indices
        for idx_key in self.indexes_from_data(data):
            self._indices[idx_key].insert(0, (t, key))
        
        return key
    
    def _get_many(self, keys):
        return map(self.deserialize, map(self._items.get, keys))
    
    def items(self, start=0, end=20, **kwargs):
        # Get the list of keys to search
        idx_keys = list(self.keys_from_keydict(kwargs))
        # If there's only one key to look through, we're in luck and we can
        # just return it properly
        if len(idx_keys) == 1:
            keys = self._indices[idx_keys[0]][start:end]
            return self._get_many((k[1] for k in keys))
        # Otherwise, we need to pull in more data and do more work in-process
        else:
            keys = []
            seen = set()
            for key in idx_keys:
                for subkey in self._indices[key][0:end]:
                    # For every key in every index that we haven't seen yet,
                    # add it and note that we've seen it.
                    if subkey[1] not in seen:
                        keys.append(subkey)
                        seen.add(subkey[1])
            # Sort the full list of keys by the timestamp
            keys.sort(key=lambda x: x[0], reverse=True)
            # Take the slice of keys that we want (start:end) and get the
            # appropriate objects, discarding the timestamp
            return self._get_many((k[1] for k in keys[start:end]))


class RedisBackend(BaseBackend):
    def __init__(self, keys=None, host=None, port=None, db=9):
        super(RedisBackend, self).__init__(keys=keys)
        from redis import Redis
        self.client = Redis(host=host, port=port, db=db)
    
    def insert(self, data, date=None):
        key = str(uuid.uuid1())
        
        self.client.set(key, self.serialize(data))
        
        serialized_key = self.serialize((coerce_ts(date), key))
        
        # Insert into the global index
        self.client.push('_all', serialized_key, head=True)
        
        # Maintain the other indices
        for idx_key in self.indexes_from_data(data):
            self.client.push(idx_key, serialized_key, head=True)
    
    def _get_many(self, keys):
        return map(self.deserialize, self.client.mget(*keys))
    
    def items(self, start=0, end=20, **kwargs):
        # Get the list of keys to search
        idx_keys = list(self.keys_from_keydict(kwargs))
        # If there's only one key to look through, we're in luck and we can
        # just return it properly
        if len(idx_keys) == 1:
            keys = map(self.deserialize,
                self.client.lrange(idx_keys[0], start, end))
            return self._get_many((k[1] for k in keys))
        # Otherwise, we need to pull in more data and do more work in-process
        else:
            keys = []
            seen = set()
            for key in idx_keys:
                subkeys = map(self.deserialize, self.client.lrange(key, 0, end))
                for subkey in subkeys:
                    # For every key in every index that we haven't seen yet,
                    # add it and note that we've seen it.
                    if subkey[1] not in seen:
                        keys.append(subkey)
                        seen.add(subkey[1])
            # Sort the full list of keys by the timestamp
            keys.sort(key=lambda x: x[0], reverse=True)
            # Take the slice of keys that we want (start:end) and get the
            # appropriate objects, discarding the timestamp
            return self._get_many((k[1] for k in keys[start:end]))


class SQLBackend(BaseBackend):
    def __init__(self, dsn=None, keys=None, table_name='stream_items', **kwargs):
        super(SQLBackend, self).__init__(keys=keys)
        from sqlalchemy import create_engine
        self.table_name = table_name
        self.engine = create_engine(dsn, **kwargs)
        self.setup_table()
    
    def setup_table(self):
        from sqlalchemy import MetaData, Table, Column, String, Text, DateTime
        index_columns = []
        for k in self.keys:
            index_columns.append(Column(k, Text, index=True))
        self.metadata = MetaData()
        self.metadata.bind = self.engine
        self.table = Table(self.table_name, self.metadata,
            Column('key', String(length=36), primary_key=True),
            Column('data', Text, nullable=False),
            Column('date', DateTime, default=datetime.datetime.now, index=True,
                nullable=False),
            *index_columns
        )
        self.table.create(bind=self.engine, checkfirst=True)
    
    def insert(self, data, date=None):
        key = str(uuid.uuid1())
        dct = {
            'key': key,
            'data': self.serialize(data),
            'date': coerce_dt(date),
        }
        for k in self.keys:
            value = data.get(k)
            if value is not None:
                value = unicode(value).encode('utf-8')
            dct[k] = value
        self.table.insert().execute(**dct)
    
    def items(self, start=0, end=20, **kwargs):
        query = self.table.select().order_by(self.table.c.date.desc())
        for key, value in kwargs.iteritems():
            if isinstance(value, list):
                values = [unicode(v).encode('utf-8') for v in value]
                query = query.where(getattr(self.table.c, key).in_(values))
            else:
                value = unicode(value).encode('utf-8')
                query = query.where(getattr(self.table.c, key)==value)
        query = query.offset(start).limit(end-start)
        result = query.execute()
        return [self.deserialize(row['data']) for row in result]