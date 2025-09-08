"""
dbConnectionEngine
==============

Author: Stanley Parmar

Description: Db connection is a Library for connecting to Postgres in
             various ways and also registering ,querying and deleting the
             users.

See the examples directory to learn about the usage.

"""
import psycopg2.pool
from backend.common.commonUtility import open_read_file_box, get_sys_args, logger
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from urllib.parse import quote_plus

# get the database configurations
logger.info("in engine")

try:
    filename = get_sys_args()
except Exception as e:
    logger.error("to get system arguments, {}".format(str(e)))

try:
    db_config = open_read_file_box(filename + '_postgres')
except Exception as e:
    logger.error("to get open read file, {}".format(str(e)))

# pool = None

try:
    encoded_username = quote_plus(db_config["db_user"])
    encoded_password = quote_plus(db_config["db_password"])
    # Initialize the PostgreSQL db
    db_url = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
        user=encoded_username,
        password=encoded_password,
        host=db_config["db_host"],
        port=db_config["db_port"],
        database=db_config["db_name"]
    )
except Exception as e:
    logger.info("entered in exception for postgress db")
    logger.error(e)

try:
    # Initialize the engine
    engine = create_engine(
        db_url,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800
    )
except Exception as e:
    logger.info("entered in exception for engine")
    logger.error(e)


def get_connection():
    """Get a connection from the pool."""
    return engine.connect()


def release_connection(conn):
    """Release a connection back to the pool."""
    conn.close()


def get_db_host():
    return filename, db_config["db_host"]
