from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.binary import Binary
from bson.objectid import ObjectId
import pickle
import gridfs

from lightflow.logger import get_logger
from .exceptions import DataStoreNotConnected

logger = get_logger(__name__)

WORKFLOW_DATA_COLLECTION_NAME = 'workflow-data'
WORKFLOW_DATA_DOCUMENT_META = 'meta'
WORKFLOW_DATA_DOCUMENT_DATA = 'data'


class DataStore:
    """ The persistent data storage for data shared during the life of a workflow.

    The DataStore is a persistent storage for all data that should be stored over the
    lifetime of a workflow and be made available to all tasks in the workflow. This
    storage is particularly useful for trigger based workflows that perform averaging
    or summing work.

    The DataStore is implemented using a MongoDB backend. For each workflow run a
    document is created and its id is used for identifying the workflow run.
    """
    def __init__(self, host, port, database):
        """ Initialise the DataStore.

        Args:
            host (str): The host on which the MongoDB server runs.
            port (int): The port on which the MongoDB server listens.
            database (str): The name of the MongoDB collection.

        Attributes:
            host (str): The host on which the MongoDB server runs.
            port (int): The port on which the MongoDB server listens.
            database (str): The name of the MongoDB collection.
        """
        self.host = host
        self.port = port
        self.database = database
        self._client = None

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
        """ Establishes a connection to the MongoDB server. """
        self._client = MongoClient(host=self.host, port=self.port)

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

    def add(self, name):
        """ Adds a new document to the data store and returns its id.

        Args:
            name (str): The name of the workflow that is attached to the new document.

        Raises:
            DataStoreNotConnected: If the data store is not connected to the server.

        Returns:
            str: The id of the newly created document.
        """
        try:
            db = self._client[self.database]
            col = db[WORKFLOW_DATA_COLLECTION_NAME]
            return str(col.insert_one({
                WORKFLOW_DATA_DOCUMENT_META: {
                    'name': name,
                    'start_time': datetime.utcnow()
                },
                WORKFLOW_DATA_DOCUMENT_DATA: {}
            }).inserted_id)

        except ConnectionFailure:
            raise DataStoreNotConnected()

    def remove(self, workflow_id):
        """ Removes a document specified by its id from the data store.

        Args:
            workflow_id (str): The id of the document that represents a workflow run.

        Raises:
            DataStoreNotConnected: If the data store is not connected to the server.
        """
        try:
            db = self._client[self.database]
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
            fs = gridfs.GridFS(db)
            return DataStoreDocument(db[WORKFLOW_DATA_COLLECTION_NAME], fs, workflow_id)

        except ConnectionFailure:
            raise DataStoreNotConnected()


class DataStoreDocument:
    """ A single data store document containing the data for a workflow run.

    The document provides methods in order to retrieve and set data in the
    persistent data store. It represents the data for a single workflow run.
    """

    def __init__(self, collection, grid_fs, workflow_id):
        """ Initialise the data store document.

        Args:
            collection: A MongoDB collection object pointing to the data store collection.
            grid_fs: A GridFS object used for splitting large, binary data into smaller
                     chunks in order to avoid the 16MB document limit of MongoDB.
            workflow_id: The id of the workflow run this document is associated with.
        """
        self._collection = collection
        self._gridfs = grid_fs
        self._workflow_id = workflow_id

    def get(self, key, default=None):
        """ Return the field specified by its key from the document data section.

        This method access the data section of the workflow document and returns the
        value for the specified key.

        Args:
            key (str): The key pointing to the value that should be retrieved. It supports
                       MongoDB's dot notation for nested fields.
            default: The default value that is returned if the key does not exist.

        Returns:
            object: The value from the field that the specified key is pointing to. If the
                    key does not exist, the default value is returned. If no default value
                    is provided and the key does not exist None is returned.
        """
        doc = self._collection.find_one({"_id": ObjectId(self._workflow_id)})
        if doc is None:
            return default

        data = doc[WORKFLOW_DATA_DOCUMENT_DATA]
        for k in key.split('.'):
            data = data[k]

        return data

    def set(self, key, value):
        """ Store a value under the specified key in the data section of the document.

        This method stores a value into the data section of the workflow data store
        document. Any existing value is overridden.

        Args:
            key (str): The key pointing to the value that should be stored/updated.
                       It supports MongoDB's dot notation for nested fields.
            value: The value that should be stored/updated.

        Returns:
            bool: True if the value could be set/updated, otherwise False.
        """
        key_notation = '.'.join([WORKFLOW_DATA_DOCUMENT_DATA, key])
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

    def push(self, key, value):
        """ Appends a value to the

        Args:
            key (str): The key pointing to the value that should be stored/updated.
                       It supports MongoDB's dot notation for nested fields.
            value: The value that should be appended to a list in the data store.

        Returns:
            bool: True if the value could be appended, otherwise False.
        """
        key_notation = '.'.join([WORKFLOW_DATA_DOCUMENT_DATA, key])
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

    def extend(self, key, values):
        """ Extends a list in the data store with the elements of values.

        Args:
            key (str): The key pointing to the value that should be stored/updated.
                       It supports MongoDB's dot notation for nested fields.
            values:

        Returns:
            bool: True if the list in the database could be extended, otherwise False.
        """
        key_notation = '.'.join([WORKFLOW_DATA_DOCUMENT_DATA, key])
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
            Binary(pickle.dumps(value), subtype=128)
