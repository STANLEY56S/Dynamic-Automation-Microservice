"""
dbDyanmicColumnsConnPool
==============

Author: Stanley Parmar

Description: Dbconnection is a Library for connecting to Postgress in
             various ways and also registering ,querying and deleteing the
             users.

See the examples directory to learn about the usage.

"""
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from backend.common.commonUtility import open_read_file_box, get_sys_args


class Database:
    def __init__(self):
        config = self.load_config()
        self.engine = create_engine(
            "postgresql+psycopg2://{}:{}@{}/{}".format(
                config['db_user'], config['db_password'], config['db_host'], config['db_name']),
            pool_size=int(config['max_conn']),
            max_overflow=int(config['min_conn'])
        )
        self.session = sessionmaker(bind=self.engine)

    @staticmethod
    def load_config():
        """Load configuration from a file."""
        filename = get_sys_args()
        db_config = open_read_file_box(filename + '_postgres')
        return db_config

    def execute_select(self, query, params=None):
        """Execute a SELECT query and return results along with column names."""
        session = self.session()
        result = session.execute(text(query), params or {})
        rows = result.fetchall()
        column_names = result.keys()
        session.close()
        # Convert rows to a list of dictionaries
        data = [dict(zip(column_names, row)) for row in rows]

        return data

    def close(self):
        """Close the database connection."""
        self.engine.dispose()


# Create a global Database instance
db = Database()
