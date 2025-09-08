"""
dbMongoConnection.py
==============
Author: Stanley Parmar
Description: DB Connection pool for Mongo Connection.
See the examples directory to learn about the usage.
"""

# dbMongoConnection.py


# Import the custom defined Libraries and functions
from pymongo import MongoClient
from backend.common.commonUtility import open_read_file_box, get_sys_args

"""
    Class Name: MongoDBConnection
    Functions: __init__
        Inputs: None
        Output: None
    Functions: get_database
        Inputs: self
        Output: db-- Getting the Mongo Database connection
    Functions: get_mongo_collection
        Inputs: self
        Output: collection-- Getting the Mongo Database collection
    Functions: close
        Inputs: self
        Output: close-- Closing the Mongo Database connection             
"""


class MongoDBConnection:
    # Self initializing for the whole session
    def __init__(self):
        # Get the system arguments to fetch project related Mongo connections only
        filename = get_sys_args()
        # Reading the config file and getting the needed variables to open the connection pool
        db_config = open_read_file_box(filename + '_mongodb')
        # Set the self which will be used for the whole session
        self.collection = None
        # Set the Mongo client URI
        self.client = MongoClient(db_config['mongo_uri'])
        # Set the Mongo Database Name
        self.db = self.client[db_config['database_name']]

    # Getting the Mongo Database connection
    def get_database(self):
        # Return the DB info in here
        return self.db

    # Getting the Mongo Database collection
    def get_mongo_collection(self, collection_name):
        # Get the Database Name
        db = self.get_database()
        # Get the collection info
        self.collection = db.get_collection(collection_name)
        return self.collection

    def get_mongo_collection_list(self):
        # Get the Database Name
        db = self.get_database()
        # Get all collections
        self.collection = db.list_collection_names()
        return self.collection

    # Closing the Mongo Database connection
    def close(self):
        # Close the Client
        self.client.close()
