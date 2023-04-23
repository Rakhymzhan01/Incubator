import os
import itertools
import orjson


class FileManagerMixin:

    def __init__(self, path: str | os.PathLike):
        self._path = path

    def read_file(self):
        with open(self._path, 'rb') as file:
            return orjson.loads(file.read())

    def write_file(self, data: dict):
        raw_content = orjson.dumps(data, default=str, option=orjson.OPT_APPEND_NEWLINE)
        with open(self._path, 'wb') as file:
            file.write(raw_content)


class _Document(FileManagerMixin):

    def __init__(self, path: str | os.PathLike):
        super().__init__(path)
        self._data: dict = self.read_file()

    def __getattr__(self, item):
        return self._data.get(item)

    def __setattr__(self, key, value):
        self._data[key] = value

    def save(self):
        self.write_file(self._data)


class Index(FileManagerMixin):
    def __init__(self, path: str | os.PathLike):
        super().__init__(path)

    def sort(self):
        with open(self._path, 'r') as file:
            data = orjson.loads(file.read())
        sorted_data = sorted(data.items(), key=lambda x: x[1])
        with open(self._path, 'w') as file:
            file.write(orjson.dumps(dict(sorted_data), default=str, option=orjson.OPT_APPEND_NEWLINE))


class Collection:
    def __init__(self, collection_name: str):
        self._collection_name = collection_name
        self._directory_path = os.path.join(os.getcwd(), collection_name)
        
        if not os.path.exists(self._directory_path):
            os.makedirs(self._directory_path)
        
        self._indexes: list[Index] = []
        self._indexed_fields = []

    def create_index_files(self, indexed_fields):
        
        for i in range(len(indexed_fields)):
            combinations = itertools.combinations(indexed_fields, i+1)
            for combination in combinations:
                index_file_name = '_'.join(combination) + '__index'
                index_file_path = os.path.join(self._directory_path, index_file_name)
                if not os.path.exists(index_file_path):
                    with open(index_file_path, 'w') as index_file:
                        index_file.write('{}')

    def load_indexes(self):
        for root, dirs, files in os.walk(self._directory_path):
            for file in files:
                if '__index' in file:
                    index_path = os.path.join(root, file)
                    self._indexes.append(Index(index_path))
                    self._indexed_fields.extend(file.replace('__index', '').split('_'))

    @classmethod
    def create_collection(cls, collection_name, indexed_fields=None):
        collection = cls(collection_name)
        if indexed_fields:
            collection.create_index_files(indexed_fields)
        collection.load_indexes()
        return collection

   def create(self, data: dict):
    document_path = os.path.join(self._directory_path, f"{data['id']}.json")
    document = _Document(document_path)
    for key, value in data.items():
        setattr(document, key, value)
    document.save()


    for index in self._indexes:
        index_data = index.read_file()
        for field in index_data:
            if field in data:
                value = data[field]
                if value not in index_data[field]:
                    index_data[field].append(value)
                    index.write_file(index_data)


    def delete(self, document: _Document):
    
    os.remove(document._path)
    
    for index in self._indexes:
        field = index._path.split('_')[-2]
        value = getattr(document, field)

        if value in index._data:
            index._data[value].remove(document._path)
            index.write_file(index._data)

    def find(self, **filters):
        filter_fields = set(filters.keys())
        index_fields = set(index._path.split('/')[-1].split('_')[:-1] for index in self._indexes)

        if filter_fields.issubset(index_fields):
            filtered_docs = None
            for field, value in filters.items():
                for index in self._indexes:
                    if index._path.endswith(f"_{field}__index"):
                        if value in index._data:
                            doc_paths = index._data[value]
                            if filtered_docs is None:
                                filtered_docs = set(doc_paths)
                            else:
                                filtered_docs &= set(doc_paths)

            
            documents = []
            for doc_path in filtered_docs:
                documents.append(_Document(doc_path))
            return documents

        else:
            documents = []
            for root, dirs, files in os.walk(f"{self._collection_name}/"):
                for file in files:
                    if file.endswith(".json"):
                        doc_path = os.path.join(root, file)
                        document = _Document(doc_path)
                        if all(getattr(document, field) == value for field, value in filters.items()):
                            documents.append(document)
            return documents



# Collection –> Index, Document_structure, Querying
# Document -> Working with files
# Document <-> File
# for each file:
#    content = f.read()
#    append content to one buffer()
#    json.loads(buffer)
# Collection –> [Document]
# Collection -> age__index.txt