"""
dbConnectionPool
==============

Author: Stanley Parmar

Description: Dbconnection is a Library for connecting to Postgress in
             various ways and also registering ,querying and deleteing the
             users.

See the examples directory to learn about the usage.

"""
import psycopg2.pool
from backend.common.commonUtility import open_read_file_box, get_sys_args, logger

# get the database configurations
logger.info("in connection pool")

try:
    filename = get_sys_args()
except Exception as e:
    logger.error("to get system arguments, {}".format(str(e)))

try:
    db_config = open_read_file_box(filename + '_postgres')
except Exception as e:
    logger.error("to get openreadfile, {}".format(str(e)))

# pool = None

try:
    # Initialize the PostgreSQL connection pool
    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=db_config['min_conn'],
        maxconn=db_config['max_conn'],
        user=db_config["db_user"],
        password=db_config["db_password"],
        host=db_config["db_host"],
        port=db_config["db_port"],
        database=db_config["db_name"]
    )
except Exception as e:
    logger.info("entered in exception")
    logger.error(e)

if pool:
    logger.info("made connection pool")


def get_connection():
    """Get a connection from the pool."""
    return pool.getconn()


def release_connection(conn):
    """Release a connection back to the pool."""
    pool.putconn(conn)


def close_pool():
    """Close all connections in the pool."""
    pool.closeall()


def get_db_host():
    return filename, db_config["db_host"]
