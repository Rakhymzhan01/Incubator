#import time 
#start = time.time()

import json
import os
from bisect import bisect_left, bisect_right


class Database:
    def __init__(self, path):
        self.path = path
        if not os.path.exists(path):
            os.mkdir(path)

    def insert(self, collection, document):
        collection_path = os.path.join(self.path, collection + '.json')
        if not os.path.exists(collection_path):
            with open(collection_path, 'w') as f:
                f.write('[]')

        with open(collection_path, 'r') as f:
            data = json.load(f)
        data.append(document)
        with open(collection_path, 'w') as f:
            json.dump(data, f)

    def find(self, collection, query=None, sort=None, range_query=None):
        collection_path = os.path.join(self.path, collection + '.json')
        if not os.path.exists(collection_path):
            return []

        with open(collection_path, 'r') as f:
            data = json.load(f)

        if query is not None:
            query_keys = query.keys()
            data = [d for d in data if all(item in d.items() for item in query.items())]

        if range_query is not None:
            range_query_key, range_query_start, range_query_stop = range_query
            range_query_data = [d[range_query_key] for d in data]
            idx1 = bisect_left(range_query_data, range_query_start)
            idx2 = bisect_right(range_query_data, range_query_stop)
            data = data[idx1:idx2]

        if sort is not None:
            data = sorted(data, key=lambda x: x[sort[0]], reverse=sort[1])

        return data

    def update(self, collection, query=None, update=None):
        collection_path = os.path.join(self.path, collection + '.json')
        if not os.path.exists(collection_path):
            return False
        with open(collection_path, 'r') as f:
            data = json.load(f)
        for document in data:
            if query is None or all(item in document.items() for item in query.items()):
                document.update(update)
        with open(collection_path, 'w') as f:
            json.dump(data, f)
        return True

    def delete(self, collection):
        collection_path = os.path.join(self.path, collection + '.json')
        if os.path.exists(collection_path):
            os.remove(collection_path)

    def list_collections(self):
        collections = []
        for file_name in os.listdir(self.path):
            if file_name.endswith('.json'):
                collections.append(file_name[:-5])
        return collections

db = Database('my_database')

db.insert('users', {'name': 'qwerty', 'age': 21, 'email': 'qwerty@gmail.com'},)

data = db.find('users', {'name': 'qwerty'})

db.update('users', {'name': 'qwerty'}, {'age': 23})

#здесь я написал бин поиск что искать за log(n) 
