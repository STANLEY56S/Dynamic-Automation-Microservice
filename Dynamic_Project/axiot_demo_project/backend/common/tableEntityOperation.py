"""
tableEntityOperation.py
==============

Author: Stanley Parmar
Description: Module to handle database operations including creating, updating, and deleting records.
"""

# tableEntityOperation.py


import traceback
import psycopg2
from psycopg2 import sql

from backend.common.entityOperation import get_schema_columns
from backend.dbConnectionPool import get_connection, release_connection
from backend.common.commonUtility import (debug_print, logger)
from backend.jsonResponse import ResponseCode


# Global Query Configuration -- Direct Understanding
query_config = {
    'get_schema_column': "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{table_name}';",
    'is_table_exist': "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}');",
    'create_table': "DROP TABLE IF EXISTS {table_name} CASCADE; CREATE TABLE {table_name} ({columns});",
    'add_column': "ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
}

def close_conn_cursor(con, cur, is_new_cur = True):
    if cur and is_new_cur:
        cur.close()
    if con and is_new_cur:
        release_connection(con)


def handle_database_exception(e):
    if isinstance(e, psycopg2.Error):
        # Log the error details
        logger.error("PostgreSQL Error: pgcode=%s, pgerror=%s, details=%s"
                     % (e.pgcode, e.pgerror, e.diag.message_detail))

        # Handle specific pgcodes
        if e.pgcode == '23505':  # UniqueViolation
            return ResponseCode.create_response("DUPLICATE_KEY")
        elif e.pgcode == '23503':  # ForeignKeyViolation
            return ResponseCode.create_response("FOREIGN_KEY_VIOLATION")
        elif e.pgcode == '23502':  # NotNullViolation
            return ResponseCode.create_response("NOT_NULL_VIOLATION")

    # Log unhandled exceptions
    logger.warning("Unhandled exception: %s", str(e))
    return ResponseCode.create_response("DATABASE_ERROR")

# get the primary column From postgres
def get_next_sequence_value(table_name, id_column, cur=None):

    con = None
    is_new_cur = False

    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        # Build default sequence name (PostgreSQL default: {table}_{column}_seq)
        sequence_name = "{}_{}_seq".format(table_name, id_column)

        # insert query
        query = sql.SQL("SELECT NEXTVAL(%s)")

        # Execute the query
        cur.execute(query, (sequence_name,))

        # Fetch once and reuse
        result = cur.fetchone()[0]
        print(result)
        return result

    except psycopg2.Error as e:
        traceback.print_exc()
        return handle_database_exception(e)

    except Exception as e:
        debug_print(f"Error in get_next_sequence_value: {str(e)}")
        logger.warning(f"Error in get_next_sequence_value: {str(e)}")
        traceback.print_exc()

    finally:
        close_conn_cursor(con, cur, is_new_cur)


def is_table_exist(table_name, cur=None):
    con = None
    is_new_cur = False

    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        # Query Built For Check table is exist
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = {}
            ) """).format(sql.Literal(table_name))

        # Execute the query
        cur.execute(query)

        # return the value boolean
        return cur.fetchone()[0]

    except psycopg2.Error as e:
        traceback.print_exc()
        return handle_database_exception(e)

    except Exception as e:
        debug_print(f"Error in is_table_exist: {str(e)}")
        logger.warning(f"Error in is_table_exist: {str(e)}")
        traceback.print_exc()

    finally:
        close_conn_cursor(con, cur, is_new_cur)


def create_primary_key(table_name, column_name_list, cur=None):
    con = None
    is_new_cur = False
    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        cur = con.cursor()
        # Create the CREATE TABLE query
        query = sql.SQL("ALTER TABLE  {} ADD CONSTRAINT {} PRIMARY KEY  ({});").format(
            sql.Identifier(table_name),
            sql.Identifier(table_name + "_pkey"),
            sql.SQL(', ').join(map(sql.Identifier, column_name_list))
        )
        debug_print("Query check: {}".format(cur.mogrify(query).decode('utf-8')))

        # Execute the query
        cur.execute(query)

        con.commit()

        debug_print("Table created.")

    except Exception as e:
        if con:
            con.rollback()
        traceback.print_exc()
        debug_print("Failed to fetch create_table Exception records: {}".format(e))
        raise e
    finally:
        close_conn_cursor(con, cur, is_new_cur)

def create_trigger(table_name, cur=None):
    con = None
    is_new_cur = False
    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        cur = con.cursor()
        # Create the CREATE TABLE query
        query = sql.SQL("CREATE OR REPLACE FUNCTION fn_{}_table_changes() RETURNS TRIGGER "
                        " AS $$ BEGIN IF TG_OP = 'INSERT' THEN RAISE NOTICE "
                        " 'Old : %, New : %', OLD.tenant_id, NEW.tenant_id; "
                        " NEW.created_at := CURRENT_TIMESTAMP; NEW.created_by := CURRENT_USER; "
                        " NEW.updated_at := CURRENT_TIMESTAMP; NEW.updated_by := CURRENT_USER;RETURN NEW;"
                        " ELSIF TG_OP = 'UPDATE' THEN RAISE NOTICE 'Old : %, New : %', OLD.tenant_id, "
                        " NEW.tenant_id; NEW.updated_at := CURRENT_TIMESTAMP; NEW.updated_by := CURRENT_USER;"
                        " RETURN NEW;ELSIF TG_OP = 'DELETE' THEN RAISE NOTICE 'Old : %, New : %', OLD.tenant_id, "
                        " NEW.tenant_id;RETURN OLD;END IF;END;$$ LANGUAGE 'plpgsql';"
                        " CREATE OR REPLACE TRIGGER trg_{}_insert "
                        " AFTER INSERT ON {} FOR EACH ROW EXECUTE FUNCTION  "
                        " fn_{}_table_changes() ; "
                        " CREATE OR REPLACE TRIGGER trg_{}_update "
                        " BEFORE UPDATE ON {} FOR EACH ROW EXECUTE FUNCTION  "
                        " fn_{}_table_changes() ;"
                        " CREATE OR REPLACE TRIGGER trg_{}_delete "
                        " BEFORE DELETE ON {} FOR EACH ROW EXECUTE FUNCTION  "
                        " fn_{}_table_changes() ; ").format(
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower()),
            sql.SQL(str(table_name).lower())
        )
        # debug_print("Query check: {}".format(cursor.mogrify(query).decode('utf-8')))

        # Execute the query
        cur.execute(query)

        con.commit()

        debug_print("Trigger created.")

    except Exception as e:
        if con:
            con.rollback()
        traceback.print_exc()
        debug_print("Failed to fetch create_table Exception records: {}".format(e))
        raise e
    finally:
        close_conn_cursor(con, cur, is_new_cur)

def create_table(table_name, columns_list, cur=None):
    con = None
    is_new_cur = False
    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        # Create the CREATE TABLE query
        query = sql.SQL("DROP TABLE IF EXISTS {} CASCADE;CREATE TABLE IF NOT EXISTS {} ({});"
                        " ALTER TABLE IF EXISTS {} OWNER to postgres; "
                        " ALTER TABLE IF EXISTS {} OWNER to powerbiusr; ").format(
            sql.Identifier(table_name),
            sql.Identifier(table_name),
            sql.SQL(', ').join(columns_list),
            sql.Identifier(table_name),
            sql.Identifier(table_name),
        )

        # Execute the query
        cur.execute(query)

        # Commit the changes
        con.commit()

        print("Table created.")

    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)

    except Exception as e:
        if con:
            con.rollback()
        debug_print(f"Error in create_table: {str(e)}")
        logger.warning(f"Error in create_table: {str(e)}")
        traceback.print_exc()

    finally:
        close_conn_cursor(con, cur, is_new_cur)


def add_column(table_name, column_name, column_type, cur=None):
    con = None
    is_new_cur = False
    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        # Get the existing columns in the table
        existing_columns = get_schema_columns(table_name)

        if column_name not in existing_columns:

            query = sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                sql.Identifier(table_name),
                sql.Identifier(column_name),
                sql.SQL(column_type)
            )

            # Execute the query
            cur.execute(query)

    except Exception as e:
        debug_print(f"Error in add_column: {str(e)}")
        logger.warning(f"Error in add_column: {str(e)}")
        traceback.print_exc()

    finally:
        close_conn_cursor(con, cur, is_new_cur)


def add_bulk_column(table_name, columns_list, cur=None):

    con = None
    is_new_cur = False
    try:
        if cur is None:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        # Get the existing columns in the table
        existing_columns = get_schema_columns(table_name)

        # Add Column List if Not in existing clm list
        columns_to_add = [
            sql.SQL("ADD COLUMN {} {}").format(sql.Identifier(clm_name), sql.SQL(clm_type))
            for clm_name, clm_type in columns_list
            if clm_name not in existing_columns
        ]

        # process only if add column
        if columns_to_add:
            
            # build Add column clause Query
            query = sql.SQL("ALTER TABLE {} {}").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(columns_to_add)
            )

            # Execute the query
            cur.execute(query)

            # Commit the changes
            con.commit()

    except Exception as e:
        debug_print(f"Error in add_bulk_column: {str(e)}")
        logger.warning(f"Error in add_bulk_column: {str(e)}")
        traceback.print_exc()

    finally:
        close_conn_cursor(con, cur, is_new_cur)