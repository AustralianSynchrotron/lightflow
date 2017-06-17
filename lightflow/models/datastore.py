from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.binary import Binary
from bson.objectid import ObjectId
import pickle
from gridfs import GridFS

from lightflow.logger import get_logger
from .mongo_proxy import MongoClientProxy, GridFSProxy
from .exceptions import DataStoreNotConnected, \
    DataStoreGridfsIdInvalid, DataStoreDecodeUnknownType

logger = get_logger(__name__)

WORKFLOW_DATA_COLLECTION_NAME = 'workflow-data'


class DataStoreDocumentSection:
    """ The different sections the data store document contains """
    Meta = 'meta'
    Data = 'data'


class DataStore:
    """ The persistent data storage for data shared during the life of a workflow.

    The DataStore is a persistent storage for all data that should be stored over the
    lifetime of a workflow and be made available to all tasks in the workflow. This
    storage is particularly useful for trigger based workflows that perform averaging
    or summing work.

    The DataStore is implemented using a MongoDB backend. For each workflow run a
    document is created and its id is used for identifying the workflow run.

    A proxy for the MongoClient is used to catch the AutoReconnect exception and handle
    it gracefully. Please note:
    """
    def __init__(self, host, port, database, *, auto_connect=False):
        """ Initialize the DataStore.

        Args:
            host (str): The host on which the MongoDB server runs.
            port (int): The port on which the MongoDB server listens.
            database (str): The name of the MongoDB collection.
            auto_connect (bool): Set to True to connect to the MongoDB database.

        Attributes:
            host (str): The host on which the MongoDB server runs.
            port (int): The port on which the MongoDB server listens.
            database (str): The name of the MongoDB collection.
        """
        self.host = host
        self.port = port
        self.database = database

        self._client = None
        if auto_connect:
            self.connect()

    @property
    def is_connected(self):
        """ Returns the connection status of the data store.

        Returns:
            bool: True if the data store is connected to the MongoDB server.
        """
        if self._client is not None:
            try:
                self._client.server_info()
            except ConnectionFailure:
                return False
            return True
        else:
            return False

    def connect(self):
        """ Establishes a connection to the MongoDB server.

        Use the MongoProxy library in order to automatically handle AutoReconnect
        exceptions in a gracefull and reliable way.
        """
        self._client = MongoClientProxy(MongoClient(host=self.host, port=self.port))

    def disconnect(self):
        """ Disconnect from the MongoDB server. """
        if self._client is not None:
            self._client.close()

    def exists(self, workflow_id):
        """ Checks whether a document with the specified workflow id already exists.

        Args:
            workflow_id (str): The workflow id that should be checked.

        Raises:
            DataStoreNotConnected: If the data store is not connected to the server.

        Returns:
            bool: True if a document with the specified workflow id exists.
        """
        try:
            db = self._client[self.database]
            col = db[WORKFLOW_DATA_COLLECTION_NAME]
            return col.find_one({"_id": ObjectId(workflow_id)}) is not None

        except ConnectionFailure:
            raise DataStoreNotConnected()

    def add(self, payload=None):
        """ Adds a new document to the data store and returns its id.

        Args:
            payload (dict): Dictionary of initial data that should be stored
                            in the new document in the meta section.

        Raises:
            DataStoreNotConnected: If the data store is not connected to the server.

        Returns:
            str: The id of the newly created document.
        """
        try:
            db = self._client[self.database]
            col = db[WORKFLOW_DATA_COLLECTION_NAME]
            return str(col.insert_one({
                DataStoreDocumentSection.Meta:
                    payload if isinstance(payload, dict) else {},
                DataStoreDocumentSection.Data: {}
            }).inserted_id)

        except ConnectionFailure:
            raise DataStoreNotConnected()

    def remove(self, workflow_id):
        """ Removes a document specified by its id from the data store.

        All associated GridFs documents are deleted as well.

        Args:
            workflow_id (str): The id of the document that represents a workflow run.

        Raises:
            DataStoreNotConnected: If the data store is not connected to the server.
        """
        try:
            db = self._client[self.database]
            fs = GridFSProxy(GridFS(db.unproxied_object))

            for grid_doc in fs.find({"workflow_id": workflow_id},
                                    no_cursor_timeout=True):
                fs.delete(grid_doc._id)

            col = db[WORKFLOW_DATA_COLLECTION_NAME]
            return col.delete_one({"_id": ObjectId(workflow_id)})

        except ConnectionFailure:
            raise DataStoreNotConnected()

    def get(self, workflow_id):
        """ Returns the document for the given workflow id.

        Args:
            workflow_id (str): The id of the document that represents a workflow run.

        Raises:
            DataStoreNotConnected: If the data store is not connected to the server.

        Returns:
            DataStoreDocument: The document for the given workflow id.
        """
        try:
            db = self._client[self.database]
            fs = GridFSProxy(GridFS(db.unproxied_object))
            return DataStoreDocument(db[WORKFLOW_DATA_COLLECTION_NAME], fs, workflow_id)

        except ConnectionFailure:
            raise DataStoreNotConnected()


class DataStoreDocument:
    """ A single data store document containing the data for a workflow run.

    The document provides methods in order to retrieve and set data in the
    persistent data store. It represents the data for a single workflow run.
    """

    def __init__(self, collection, grid_fs, workflow_id):
        """ Initialize the data store document.

        Args:
            collection: A MongoDB collection object pointing to the data store collection.
            grid_fs: A GridFS object used for splitting large, binary data into smaller
                     chunks in order to avoid the 16MB document limit of MongoDB.
            workflow_id: The id of the workflow run this document is associated with.
        """
        self._collection = collection
        self._gridfs = grid_fs
        self._workflow_id = workflow_id

    def get(self, key, default=None, *, section=DataStoreDocumentSection.Data):
        """ Return the field specified by its key from the specified section.

        This method access the specified section of the workflow document and returns the
        value for the given key.

        Args:
            key (str): The key pointing to the value that should be retrieved. It supports
                       MongoDB's dot notation for nested fields.
            default: The default value that is returned if the key does not exist.
            section (DataStoreDocumentSection): The section from which the data should
                                                be retrieved.

        Returns:
            object: The value from the field that the specified key is pointing to. If the
                    key does not exist, the default value is returned. If no default value
                    is provided and the key does not exist None is returned.
        """
        key_notation = '.'.join([section, key])
        try:
            return self._decode_value(self._data_from_dotnotation(key_notation, default))
        except KeyError:
            return None

    def set(self, key, value, *, section=DataStoreDocumentSection.Data):
        """ Store a value under the specified key in the given section of the document.

        This method stores a value into the specified section of the workflow data store
        document. Any existing value is overridden. Before storing a value, any linked
        GridFS document under the specified key is deleted.

        Args:
            key (str): The key pointing to the value that should be stored/updated.
                       It supports MongoDB's dot notation for nested fields.
            value: The value that should be stored/updated.
            section (DataStoreDocumentSection): The section from which the data should
                                                be retrieved.

        Returns:
            bool: True if the value could be set/updated, otherwise False.
        """
        key_notation = '.'.join([section, key])

        try:
            self._delete_gridfs_data(self._data_from_dotnotation(key_notation,
                                                                 default=None))
        except KeyError:
            logger.info('Adding new field {} to the data store'.format(key_notation))

        result = self._collection.update_one(
            {"_id": ObjectId(self._workflow_id)},
            {
                "$set": {
                    key_notation: self._encode_value(value)
                },
                "$currentDate": {"lastModified": True}
            }
        )
        return result.modified_count == 1

    def push(self, key, value, *, section=DataStoreDocumentSection.Data):
        """ Appends a value to a list in the specified section of the document.

        Args:
            key (str): The key pointing to the value that should be stored/updated.
                       It supports MongoDB's dot notation for nested fields.
            value: The value that should be appended to a list in the data store.
            section (DataStoreDocumentSection): The section from which the data should
                                                be retrieved.

        Returns:
            bool: True if the value could be appended, otherwise False.
        """
        key_notation = '.'.join([section, key])
        result = self._collection.update_one(
            {"_id": ObjectId(self._workflow_id)},
            {
                "$push": {
                    key_notation: self._encode_value(value)
                },
                "$currentDate": {"lastModified": True}
            }
        )
        return result.modified_count == 1

    def extend(self, key, values, *, section=DataStoreDocumentSection.Data):
        """ Extends a list in the data store with the elements of values.

        Args:
            key (str): The key pointing to the value that should be stored/updated.
                       It supports MongoDB's dot notation for nested fields.
            values (list): A list of the values that should be used to extend the list
                           in the document.
            section (DataStoreDocumentSection): The section from which the data should
                                                be retrieved.

        Returns:
            bool: True if the list in the database could be extended, otherwise False.
        """
        key_notation = '.'.join([section, key])
        if not isinstance(values, list):
            return False

        result = self._collection.update_one(
            {"_id": ObjectId(self._workflow_id)},
            {
                "$push": {
                    key_notation: {"$each": self._encode_value(values)}
                },
                "$currentDate": {"lastModified": True}
            }
        )
        return result.modified_count == 1

    def _data_from_dotnotation(self, key, default=None):
        """ Returns the MongoDB data from a key using dot notation.

        Args:
            key (str): The key to the field in the workflow document. Supports MongoDB's
                       dot notation for embedded fields.
            default (object): The default value that is returned if the key
                              does not exist.

        Returns:
            object: The data for the specified key or the default value.
        """
        if key is None:
            raise KeyError('NoneType is not a valid key!')

        doc = self._collection.find_one({"_id": ObjectId(self._workflow_id)})
        if doc is None:
            return default

        for k in key.split('.'):
            doc = doc[k]

        return doc

    def _encode_value(self, value):
        """ Encodes the value such that it can be stored into MongoDB.

        Any primitive types are stored directly into MongoDB, while non-primitive types
        are pickled and stored as GridFS objects. The id pointing to a GridFS object
        replaces the original value.

        Args:
            value (object): The object that should be encoded for storing in MongoDB.

        Returns:
            object: The encoded value ready to be stored in MongoDB.
        """
        if isinstance(value, (int, float, str, bool)):
            return value
        elif isinstance(value, list):
            return [self._encode_value(item) for item in value]
        elif isinstance(value, dict):
            result = {}
            for key, item in value.items():
                result[key] = self._encode_value(item)
            return result
        else:
            return self._gridfs.put(Binary(pickle.dumps(value)),
                                    workflow_id=self._workflow_id)

    def _decode_value(self, value):
        """ Decodes the value by turning any binary data back into Python objects.

        The method searches for ObjectId values, loads the associated binary data from
        GridFS and returns the decoded Python object.

        Args:
            value (object): The value that should be decoded.

        Raises:
            DataStoreDecodingError: An ObjectId was found but the id is not a valid
                                    GridFS id.
            DataStoreDecodeUnknownType: The type of the specified value is unknown.

        Returns:
            object: The decoded value as a valid Python object.
        """
        if isinstance(value, (int, float, str, bool)):
            return value
        elif isinstance(value, list):
            return [self._decode_value(item) for item in value]
        elif isinstance(value, dict):
            result = {}
            for key, item in value.items():
                result[key] = self._decode_value(item)
            return result
        elif isinstance(value, ObjectId):
            if self._gridfs.exists({"_id": value}):
                return pickle.loads(self._gridfs.get(value).read())
            else:
                raise DataStoreGridfsIdInvalid()
        else:
            raise DataStoreDecodeUnknownType()

    def _delete_gridfs_data(self, data):
        """ Delete all GridFS data that is linked by fields in the specified data.

        Args:
            data: The data that is parsed for MongoDB ObjectIDs. The linked GridFs object
                  for any ObjectID is deleted.
        """
        if isinstance(data, ObjectId):
            if self._gridfs.exists({"_id": data}):
                self._gridfs.delete(data)
            else:
                raise DataStoreGridfsIdInvalid()
        elif isinstance(data, list):
            for item in data:
                self._delete_gridfs_data(item)
        elif isinstance(data, dict):
            for key, item in data.items():
                self._delete_gridfs_data(item)
