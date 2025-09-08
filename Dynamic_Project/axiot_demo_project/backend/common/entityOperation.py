"""
entityOperations.py
==============
Author: Stanley Parmar
Description: Module to handle database operations including creating, updating, and deleting records.
"""


import json
import psycopg2
from psycopg2 import sql
from backend.dbConnectionPool import get_connection, release_connection
from backend.common.commonUtility import open_read_file, logger, debug_print
from backend.jsonResponse import ResponseCode
from backend.common.convertingJsontoListCommonOperations import convert_into_in_compatible_string_no_quotes
import traceback

AND = " AND "


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


"""
Function Name: get_schema_columns
Inputs: table_name (str): The name of the table for which the schema is being retrieved.

Output: column_without_defaults (list): A list of columns for the given table that does not have default values.

Description:
Loads the schema for the specified table from a JSON configuration file.
"""


def get_schema_columns(table_name, cur=None, with_default=False):
    """
    Loads the schema for the specified table from the Database instance.
    """
    con = None
    is_new_cur = False
    try:
        if not cur:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True

        # Define the schema and table name
        schema_name = 'public'  # Adjust as necessary

        # Query to fetch column metadata
        query = """
        SELECT column_name, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
        """

        # Execute the query
        cur.execute(query, (schema_name, table_name))

        # Fetch all results
        columns = cur.fetchall()

        # Filter out columns with default values
        columns_without_defaults = [col[0] for col in columns if with_default or col[1] is None]

        # debug_print("Columns without default values: {}".format(columns_without_defaults))

        return columns_without_defaults
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)

    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        raise e

    finally:
        if cur and is_new_cur:
            cur.close()
        if con and is_new_cur:
            release_connection(con)


"""
Function Name: get_primary_key_columns
Inputs: table_name (str): The name of the table for which the schema is being retrieved.

Output: column_without_defaults (list): A list of columns for the given table that does not have default values.

Description:
Loads the schema for the specified table from a JSON configuration file.
"""


def get_primary_key_columns(table_name=None):
    conn = None
    cursor = None

    # Define the schema and table name
    schema_name = 'public'  # Adjust as necessary
    constraint_type = 'PRIMARY KEY'
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Query to fetch primary key column metadata
        if table_name:
            query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = %s
              AND tc.table_schema = %s
              AND tc.table_name = %s;
            """

            # Execute the query
            cursor.execute(query, (constraint_type, schema_name, table_name))
            # Fetch all results
            primary_key_columns = cursor.fetchall()

            # Extract column names from the results
            primary_key_column_names = [col[0] for col in primary_key_columns]

            # debug_print("Primary key columns: {}".format(primary_key_column_names))

            return primary_key_column_names
        else:
            query = """
            SELECT
                t.table_name,
                c.column_name
            FROM
                information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                JOIN information_schema.columns c
                    ON c.table_schema = ccu.table_schema
                    AND c.table_name = ccu.table_name
                    AND c.column_name = ccu.column_name
                JOIN information_schema.tables t
                    ON t.table_schema = c.table_schema
                    AND t.table_name = c.table_name
            WHERE
                tc.constraint_type = %s and 
                tc.table_schema = %s
            ORDER BY
                t.table_schema,
                t.table_name,
                c.ordinal_position;
                """
            cursor.execute(query, (constraint_type, schema_name))
            # Fetch all results
            primary_key_columns = cursor.fetchall()
            result = [
                {
                    "module_name": row[0],
                    "value": row[1]
                }
                for row in primary_key_columns
            ]
            return result
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to create record: {}".format(str(e)))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


def check_product_exists(product_table, product_data):
    config = open_read_file('resources', '', 'general')
    product_active_status = config['product_active_status']
    conn = None
    cursor = None

    try:
        product_id = get_primary_key_columns(product_table)[0]
        conn = get_connection()
        cursor = conn.cursor()
        check_product_query = sql.SQL("""
            SELECT 1 FROM {product_table} WHERE {product_id} = %s AND {product_active_status} = TRUE
        """).format(
            product_table=sql.Identifier(product_table),
            product_id=sql.Identifier(product_id),
            product_active_status=sql.Identifier(product_active_status)
        )
        cursor.execute(check_product_query, (product_data[product_id],))
        product_exists = cursor.fetchone()

        if not product_exists:
            return False
        else:
            return True
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to fetch records: {}".format(str(e)))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: check_string_for_a_match
Inputs:
- data (str): JSON string containing the data for the record to be created.
- module_data_json: Module Json of what the validation had to be done.

Output: whether the match is present or not

Description:
Check if the string contains commas and then process it based on that.
"""


def check_string_for_a_match(input_string, validation_string):
    status = False
    # Check if the string contains commas
    if ',' in input_string:
        # If comma-separated, split the string by commas and process each part
        values = input_string.split(',')
        for value in values:
            if value.strip() not in validation_string:
                status = True
                return status  # Exit after finding True
    else:
        # If no commas, check the string directly
        if input_string.strip() not in validation_string:
            status = True
    return status


"""
    Function Name: get_parent_product_id
    Inputs:
       

    Output: 
       
    Description:
    get_parent_product_id
"""


def get_parent_product_id(product_id):
    try:
        config = open_read_file('resources', '', 'general')
        query = config['product_client_query_base'].format(product_id=product_id)

        debug_print("query : {} ".format(query))
        products = fetch_record_search_json("", "", "", "",
                                           "", query)

        return products
    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        logger.warning(e)


"""
Function Name: check_string_for_data_match
Inputs:
- data (str): JSON string containing the data for the record to be created.
- module_data_json: Module Json of what the validation had to be done.

Output: whether the match is present or not

Description:
Check if the string contains commas and then process it based on that.
"""


def check_string_for_data_match(input_string, validation_string):
    # Removing the table prefix before proceeding further
    parsed_values_colon = input_string.split(":")

    # debug_print("parsed_values_colon:{}:: {}\n" .format(parsed_values_colon, validation_string))

    # Split the string by comma
    dynamic_table_where = parsed_values_colon[1].split(',')

    # First part is the table name
    table_name = dynamic_table_where[0].strip()

    # Build the WHERE clause dynamically, check for comma-separated values
    where_clause_parts = []

    where_conditions = dynamic_table_where[1:]

    # debug_print("where_conditions:{}\n".format(where_conditions))
    # debug_print("validation_string:{}\n".format(validation_string))

    for col, val in zip(where_conditions, validation_string):
        if col == 'product_id':
            # AS 20/02/25 modified this to call the new proc to get the data effectively for CPCB and NON CPCB
            # Parsing of the product ID from the JSON data
            products_ids = get_parent_product_id(val)
            products_id = get_record_client(products_ids)
            # debug_print("products_ids before: {}".format(products_ids))
            # debug_print("products_id before: {}".format(products_id))
            val = convert_into_in_compatible_string_no_quotes(products_id)
            # debug_print("products_ids after: {}".format(val))
            # AS 20/02/25 modified this to call the new proc to get the data effectively for CPCB and NON CPCB

        # Check if the value contains a comma
        if ',' in val:
            # If there is a comma, use the IN operator
            values_list = val.split(',')
            formatted_values = "', '".join(values_list)  # Add single quotes around each value
            where_clause_parts.append("{} IN ('{}')".format(col, formatted_values))
        else:
            # Otherwise, use the equal sign
            where_clause_parts.append("{} = '{}'".format(col, val))

    # Join the parts with 'AND' to form the full WHERE clause
    where_clause = ' AND '.join(where_clause_parts)

    # Construct the final SQL query
    sql_query = "SELECT COUNT(1) FROM {} WHERE {}".format(table_name, where_clause)

    # debug_print("sql_query:{}\n".format(sql_query))

    # Fetch records for each product_id
    records = fetch_record_search_json(None,
                                       None, None, None, None, sql_query)

    # debug_print("records:{}\n".format(records))
    return records


"""
Function Name: duplicate_records
Inputs:
Output: 

Description:
duplicate records of the data
"""


def duplicate_records(table_name, filters, product_id, user_id=None):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Prepare the WHERE clause dynamically from the JSON filters
        filter_conditions = []
        filter_values = []

        for column, value in filters.items():
            filter_conditions.append("{} = %s".format(column))
            filter_values.append(value)

        # Construct the SELECT query
        where_clause = " AND ".join(filter_conditions)
        select_query = " SELECT * FROM {} WHERE {}".format(table_name, where_clause)

        # Execute the SELECT query
        cursor.execute(select_query, filter_values)
        rows = cursor.fetchall()

        if rows:
            # Step 2: Prepare the INSERT query, including the manual 'id' value
            columns = [desc[0] for desc in cursor.description]  # Get column names
            columns.remove('product_id')  # Remove 'product_id' from the column list since it's manually set
            # debug_print("user_id: {}".format(user_id))
            if user_id:
                columns.remove('user_id')  # Remove 'user_id' from the column list since it's manually set

                # debug_print("columns: {}".format(columns))
                # debug_print("len(columns): {}".format(len(columns)))
                # Prepare the insert query dynamically (including 'id')
                insert_query = (sql.SQL("INSERT INTO {table} (product_id, user_id, {fields}) VALUES (%s, %s, {values})").
                format(
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join([sql.Placeholder()] * (len(columns)))
                ))
                # debug_print(cursor.mogrify(insert_query).decode('utf-8'))
            else:
                insert_query = (sql.SQL("INSERT INTO {table} (product_id,  {fields}) VALUES (%s,  {values})").
                format(
                    table=sql.Identifier(table_name),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join([sql.Placeholder()] * (len(columns)))
                ))

            # Step 3: Insert each row as a duplicate, with the manual 'id' value
            for row in rows:
                values_to_insert = ""
                values_to_insert = (product_id,)  # Add manual 'product_id' at the start of the row values
                if user_id:
                    values_to_insert = values_to_insert + (user_id,) + row[2:]
                else:
                    values_to_insert = values_to_insert + row[1:]
                # debug_print(product_id)
                # debug_print(cursor.mogrify(insert_query).decode('utf-8'))
                # debug_print("values_to_insert======= {}".format(values_to_insert))
                # debug_print(cursor.mogrify(insert_query).decode('utf-8'))
                # added the below to handle if json columns are present on the DB while cloning
                values_to_insert = [json.dumps(v) if isinstance(v, dict) else v for v in values_to_insert]
                cursor.execute(insert_query, values_to_insert)

            # Commit the transaction to persist changes
            conn.commit()

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()
        return ResponseCode.create_response("SAVE_SUCCESSFULLY")
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to create record: {}".format(e))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: create_record
Inputs:
- data (str): JSON string containing the data for the record to be created.
- table_name (str): The name of the table where the record will be inserted.

Output: None

Description:
Creates a new record in the specified table. Fields ending with 'password' are hashed before insertion.
"""


def create_record(data, table_name,is_json=None):
    record_data = json.loads(data)
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        columns = get_schema_columns(table_name, cursor)
        # debug_print("columns: {}".format(columns))

        # Ensure any field ending with 'password' is hashed
        for key in record_data:
            if key.endswith('password'):
                if record_data[key] is not None:
                    record_data[key] = hash_password(record_data[key])

        insert_query = sql.SQL(
            "INSERT INTO public.{table} ({fields}) VALUES ({values}) RETURNING *"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join(sql.Placeholder(column) for column in columns)
        )
        # debug_print(cursor.mogrify(insert_query, record_data).decode('utf-8'))
        cursor.execute(insert_query, record_data)
        inserted_row = cursor.fetchone()
        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]
        # Filter out columns ending with "password"
        filtered_columns = [col for col in column_names if 'password' not in col.lower()]
        filtered_row = {col: value for col, value in zip(column_names, inserted_row) if col in filtered_columns}
        response_data = {
            "result": filtered_row
        }
        conn.commit()
        if is_json:
            return filtered_row
        else:
            return ResponseCode.create_response("SAVE_SUCCESSFULLY", extra_data=response_data)
    except psycopg2.Error as e:
        print("------------------------------->",e)
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        print("------------------------------->",e)
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to create record: {}".format(e))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: create_record_primary_key
Inputs:
- data (str): JSON string containing the data for the record to be created.
- table_name (str): The name of the table where the record will be inserted.

Output: None

Description:
Creates a new record in the specified table using only primary column. 
Fields ending with 'password' are hashed before insertion.
"""


def create_record_primary_key(data, table_name):
    record_data = json.loads(data)
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        columns = get_primary_key_columns(table_name)
        # debug_print("columns: {}".format(columns))

        # Ensure any field ending with 'password' is hashed
        for key in record_data:
            if key.endswith('password'):
                record_data[key] = hash_password(record_data[key])

        insert_query = sql.SQL(
            "INSERT INTO public.{table} ({fields}) VALUES ({values})"
        ).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join(sql.Placeholder(column) for column in columns)
        )
        # debug_print(cursor.mogrify(insert_query, record_data).decode('utf-8'))
        cursor.execute(insert_query, record_data)
        conn.commit()
        debug_print("Record created successfully.")
        return ResponseCode.create_response("SAVE_SUCCESSFULLY")
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to create record: {}".format(e))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: update_record
Inputs:
- record_id (str): The ID of the record to be updated.
- update_by (str): The field by which you want to update.
- data (str): JSON string containing the updated data for the record.
- table_name (str): The name of the table where the record exists.

Output: None

Description:
Updates an existing record in the specified table. Fields ending with 'password' are hashed before updating.
"""


def update_record(record_id, update_fields, data, table_name, is_json=None):
    conn = None
    cursor = None
    record_data = json.loads(data)
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Check if update_fields is a tuple, if not set it to record_id
        if not isinstance(update_fields, tuple):
            update_fields = (update_fields,)
            key_value = (record_id,)
        else:
            key_value = record_id

        # Construct the where clause
        where_clauses = [sql.SQL("{update_by} = %s").format(update_by=sql.Identifier(update_by)) for update_by in
                         update_fields]
        where_clause = sql.SQL(AND).join(where_clauses)

        # Check if the record exists
        check_record_query = sql.SQL(
            "SELECT 1 FROM public.{table} WHERE {where_clause}"
        ).format(
            table=sql.Identifier(table_name),
            where_clause=where_clause
        )

        cursor.execute(check_record_query, key_value)
        record_exists = cursor.fetchone()

        if not record_exists:
            debug_print("Record ID does not exist.")
            raise ValueError("Record ID {} does not exist.".format(record_id))

        columns = get_schema_columns(table_name, cursor)
        set_clauses = [sql.SQL("{column} = %s").format(column=sql.Identifier(column)) for column in columns if
                       column in record_data]
        set_clause = sql.SQL(", ").join(set_clauses)

        # Ensure any field ending with 'password' is hashed
        for key in record_data:
            if key.endswith('password'):
                record_data[key] = hash_password(record_data[key])

        update_query = sql.SQL(
            "UPDATE public.{table} SET {set_clause} WHERE {where_clause}"
        ).format(
            table=sql.Identifier(table_name),
            set_clause=set_clause,
            where_clause=where_clause
        )

        # Create a list of parameters for the query
        params = [record_data[column] for column in columns if column in record_data] + list(key_value)

        cursor.execute(update_query, params)
        conn.commit()
        debug_print("Record updated successfully.")
        if is_json:
            return record_data
        else:
            return ResponseCode.create_response("UPDATE_SUCCESSFULLY")
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to update record: {}".format(str(e)))
        raise e

    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: update_record_one
Inputs:
- record_id (str): The ID of the record to be updated.
- update_by (str): The field by which you want to update.
- data (str): JSON string containing the updated data for the record.
- table_name (str): The name of the table where the record exists.

Output: None

Description:
Updates an existing record in the specified table. Fields ending with 'password' are hashed before updating.
"""


def update_record_one(where_column, where_column_value, update_column, update_column_value, table_name):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # Define the update query
        update_query = (sql.SQL(" UPDATE {table} SET {update_column}= %s"
                                " WHERE {where_column}= %s")
        .format(
            table=sql.Identifier(table_name),
            update_column=sql.Identifier(update_column),
            where_column=sql.Identifier(where_column),
        ))
        # Use mogrify to create a formatted query string
        formatted_query = cursor.mogrify(update_query,
                                         (str(update_column_value), str(where_column_value))
                                         ).decode('utf-8')

        # debug_print("Formatted Update Query: {} ".format(formatted_query))

        cursor.execute(update_query, (str(update_column_value), str(where_column_value)))
        conn.commit()

        debug_print("Record updated successfully.")
    except psycopg2.Error as e:
        debug_print('Error psycopg2 getting update_record_one: : \n {}'.format(str(e)))
        logger.warning("Error psycopg2 getting update_record_one: {}".format(str(e)))
        traceback.print_exc()
        return e
    except Exception as e:
        if conn:
            conn.rollback()
        debug_print('Error Exception getting update_record_one: : \n {}'.format(str(e)))
        raise e

    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: delete_record
Inputs:
- record_id (str): The ID of the record to be deleted.
- table_name (str): The name of the table where the record exists.
- deleteBy(int/str): The field you want to delete by

Output: None

Description:
Deletes an existing record from the specified table.
"""


def delete_record(record_id, delete_by, table_name):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # debug_print(
        #     "Record deleted successfully.record_id, delete_by, table_name : {} {} {}".format(record_id, delete_by,
        #                                                                                      table_name))
        # Check if delete_by is a tuple, if not set it to record_id
        if not isinstance(delete_by, tuple):
            delete_by = (delete_by,)
            record_id = (record_id,)

        # Construct the where clause
        where_clauses = [sql.SQL("{delete_field} = %s").format(delete_field=sql.Identifier(field)) for field in
                         delete_by]
        where_clause = sql.SQL(AND).join(where_clauses)
        # Check if the record exists
        check_record_query = sql.SQL(
            "SELECT 1 FROM public.{table} WHERE {where_clause}"
        ).format(
            table=sql.Identifier(table_name),
            where_clause=where_clause
        )
        # debug_print("check_record_query: {}".format(check_record_query))
        cursor.execute(check_record_query, record_id)
        record_exists = cursor.fetchone()

        if not record_exists:
            logger.info("No record found")
            return ResponseCode.create_response("NO_DATA_FOUND")

        delete_query = sql.SQL(
            "DELETE FROM public.{table} WHERE {where_clause}"
        ).format(
            table=sql.Identifier(table_name),
            where_clause=where_clause
        )
        # debug_print("delete_query: {}".format(delete_query))

        cursor.execute(delete_query, record_id)
        conn.commit()
        debug_print("Record deleted successfully.")
        return ResponseCode.create_response("DELETE_SUCCESSFULLY")
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to delete record: {}".format(str(e)))
        raise e

    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
Function Name: fetch_data_by_id
Inputs:
- data (str): JSON string containing the updated data for the record.
- query(string): A direct query
Output: list or dict: The records fetched from the table.

Description:
Fetches records from the query. If criteria are provided, fetches records matching the criteria; otherwise, 
fetches all records.
"""


def fetch_data_by_id(data, query, order_by=None,order_type='DESC'):
    try:
        sql_where = ""
        if data:
            for key, value in data.items():
                # debug_print("item  {} {} ".format(key, value))
                if sql_where:
                    sql_where = sql_where + " AND " + key + " = '" + value + "'"
                else:
                    sql_where = sql_where + " WHERE " + key + " = '" + value + "'"

        query = query + sql_where
        # debug_print("sql_where: {}".format(query))
        if order_by:
            query = query + " order by {} {}".format(order_by,order_type)

        result = fetch_record_with_query("", "", "", query)
        return result

    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to fetch Exception records: {}".format(e))
        raise e


"""
Function Name: fetch_record_with_query
Inputs:
- table_name (str): The name of the table where the records exist.
- criteria (dict): A dictionary containing the column-value pairs for the criteria (optional).
- column_list(str): A string of comma seperated columns you want to select
-query(string): A direct query
Output: list or dict: The records fetched from the table.

Description:
Fetches records from the specified table. If criteria are provided, fetches records matching the criteria; otherwise, 
fetches all records.
"""


def fetch_record_with_query(table_name=None, column_list="*", criteria=None, query=None, module=None, card_column=None):
    conn = None
    cursor = None

    check_table_query(table_name, query)
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if query:
            cursor.execute(query)
            records = cursor.fetchall()
            check_log_records(records)

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]

            # Filter out columns ending with "password"
            filtered_columns = [col for col in column_names if not col.endswith("password")]

            # Convert the records to a list of dictionaries excluding password columns
            records_list = [
                {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
                for record in records
            ]

            if records_list:
                return ResponseCode.create_response("SUCCESSFUL",
                                                    {"result": records_list, "record_length": len(records_list),
                                                     "card_column": card_column})
            else:
                if module:
                    extra_message = "Please add '{}' to move further.".format(module)
                    return ResponseCode.create_response("NO_DATA_FOUND", extra_message=extra_message)
                else:
                    return ResponseCode.create_response("NO_DATA_FOUND")
            # return records_list

        config = open_read_file('resources', '', 'general')
        # Handle the column list
        columns = handle_columns(column_list)
        schema = config["schema"]
        if criteria:
            where_clauses = []
            values = []

            for key, value in criteria.items():
                if isinstance(value, list):
                    placeholders = sql.SQL(', ').join(sql.Placeholder() * len(value))
                    where_clauses.append(sql.SQL("{} IN ({})").format(sql.Identifier(key), placeholders))
                    values.extend(value)
                else:
                    where_clauses.append(sql.SQL("{} = {}").format(sql.Identifier(key), sql.Placeholder()))
                    values.append(value)

            where_clause = sql.SQL(" AND ").join(where_clauses)
            query = sql.SQL("SELECT {} FROM {}.{} WHERE {}").format(
                columns,
                sql.Identifier(schema),
                sql.Identifier(table_name),
                where_clause
            )
            # debug_print("fetch_record if query:{}".format(query))
            cursor.execute(query, values)
        else:
            query = sql.SQL("SELECT {} FROM {}.{}").format(
                columns,
                sql.Identifier(schema),
                sql.Identifier(table_name)
            )
            # debug_print("fetch_record else query:{}".format(query))
            cursor.execute(query)

        records = cursor.fetchall()
        # debug_print("fetch_record records:{}".format(records))
        check_log_records(records)

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]

        # Filter out columns ending with "password"
        filtered_columns = [col for col in column_names if not col.endswith("password")]

        # Convert the records to a list of dictionaries excluding password columns
        records_list = [
            {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
            for record in records
        ]

        if records_list:
            return ResponseCode.create_response("SUCCESSFUL",
                                                {"result": records_list, "record_length": len(records_list),
                                                 "card_column": card_column})
        else:
            if module:
                extra_message = "Please add '{}' to move further.".format(module)
                return ResponseCode.create_response("NO_DATA_FOUND", extra_message=extra_message)
            else:
                return ResponseCode.create_response("NO_DATA_FOUND")

        # return records_list
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        logger.error(e)
        debug_print("Failed to fetch psycopg2 records: {}".format(e))
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to fetch Exception records: {}".format(e))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


def check_table_query(table, query):
    if not table and not query:
        return None


def check_log_records(records):
    if not records:
        logger.info("No record found")


def handle_columns(column_list):
    if column_list == "*":
        columns = sql.SQL("*")
    else:
        columns = sql.SQL(', ').join([sql.Identifier(col.strip()) for col in column_list.split(',')])
    return columns


"""
Function Name: fetch_record
Inputs:
- table_name (str): The name of the table where the records exist.
- criteria (dict): A dictionary containing the column-value pairs for the criteria (optional).

Output: list or dict: The records fetched from the table.

Description:
Fetches records from the specified table. If criteria are provided, fetches records matching the criteria; otherwise, 
fetches all records.
"""


def fetch_record(table_name, criteria=None):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        if criteria:
            where_clauses = [sql.SQL("{key} = %s").format(key=sql.Identifier(k)) for k in criteria.keys()]
            where_clause = sql.SQL(AND).join(where_clauses)
            query = sql.SQL(
                "SELECT * FROM public.{table} WHERE {where_clause}"
            ).format(
                table=sql.Identifier(table_name),
                where_clause=where_clause
            )
            # debug_print("fetch_record : {}".format(cursor.mogrify(query, tuple(criteria.values())).decode('utf-8')))
            cursor.execute(query, tuple(criteria.values()))

        else:
            query = sql.SQL("SELECT * FROM public.{table}").format(
                table=sql.Identifier(table_name)
            )
            cursor.execute(query)

        records = cursor.fetchall()
        if not records:
            logger.info("No record found")
            return ResponseCode.create_response("NO_DATA_FOUND")

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]

        # Filter out columns ending with "password"
        filtered_columns = [col for col in column_names if not col.endswith("password")]

        # Convert the records to a list of dictionaries excluding password columns
        records_list = [
            {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
            for record in records
        ]

        return records_list
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        if conn:
            conn.rollback()
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("Failed to fetch records: {}".format(str(e)))
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
    Fetches records from the specified table where any column matches the search value
    or where specific column-value pairs match.

    Args:
        table_name (str): The name of the table to search.
        search_value (str): The value to search for across all columns.
        column_filters (dict): A dictionary of column-value pairs to filter by.

    Returns:
        list: A list of records that match the search criteria.
"""


def fetch_record_search_json(table_name, search_value=None, column_filters=None, operand=None, parent_call=None,
                             query=None):
    conn = None
    cursor = None
    operand_value = None

    # debug_print("fetch_record_search_json: query{}".format(query))

    if operand:
        operand_value = operand
    else:
        operand_value = AND

    try:

        conn = get_connection()
        cursor = conn.cursor()

        if query:
            # debug_print(cursor.mogrify(query).decode('utf-8'))
            # formatted_query = sqlparse.format(query, reindent=True, keyword_case='upper')
            # debug_print(query)
            cursor.execute(query)
            records = cursor.fetchall()
            check_log_records(records)

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]

            # Filter out columns ending with "password"
            filtered_columns = [col for col in column_names if not col.endswith("password")]

            # Convert the records to a list of dictionaries excluding password columns
            records_list = [
                {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
                for record in records
            ]
            return records_list

        if not search_value and not column_filters:
            query = sql.SQL("SELECT * FROM {}").format(
                sql.Identifier(table_name)
            )
            cursor.execute(query)
            records = cursor.fetchall()

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]

            # Filter out columns ending with "password"
            filtered_columns = [col for col in column_names if not col.endswith("password")]

            # Convert the records to a list of dictionaries excluding password columns
            records_list = [
                {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
                for record in records
            ]

            return records_list

        # Get the columns of the table
        columns = get_schema_columns(table_name, cursor)

        # Prepare the query to search across all columns or specific column-value pairs
        ilike_conditions = []
        filter_conditions = []
        parameters = []

        if search_value:
            ilike_conditions = [
                sql.SQL("{}::text ILIKE %s").format(sql.Identifier(col)) for col in columns
            ]
            parameters.extend(["%" + search_value + "%"] * len(columns))

        if column_filters:
            filter_conditions = [
                sql.SQL("{} = %s").format(sql.Identifier(col)) for col in column_filters.keys()
            ]
            parameters.extend(column_filters.values())

        # Combine conditions
        combined_conditions = []
        if ilike_conditions:
            combined_conditions.append(sql.SQL("({})").format(sql.SQL(" OR ").join(ilike_conditions)))
        if filter_conditions:
            combined_conditions.extend(filter_conditions)

        # Prepare the final query
        query = sql.SQL("SELECT * FROM {} WHERE {}").format(
            sql.Identifier(table_name),
            sql.SQL(operand_value).join(combined_conditions)
        )

        # debug_print(query.as_string(conn))  # Print the query for debugging
        # debug_print(parameters)  # Print the parameters for debugging

        # Execute the query
        cursor.execute(query, parameters)
        records = cursor.fetchall()

        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]

        # Filter out columns ending with "password"
        filtered_columns = [col for col in column_names if not col.endswith("password")]

        # Convert the records to a list of dictionaries excluding password columns
        if parent_call:
            records_list = [
                {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
                for record in records
                if parent_call and record[column_names.index(parent_call)] != False
                # if record[column_names.index("product_active_status")] != False
            ]
        else:
            records_list = [
                {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
                for record in records
            ]

        return records_list

    except psycopg2.Error as e:
        debug_print("An psycopg2 error occurred: {}".format(str(e)))
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        debug_print("An error occurred: {}".format(str(e)))
        traceback.print_exc()  # This will print the full traceback, including the line number
        raise e

    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


"""
    Fetches records from the specified table where any column matches the search value
    or where specific column-value pairs match.

    Args:
        table_name (str): The name of the table to search.
        search_value (str): The value to search for across all columns.
        column_filters (dict): A dictionary of column-value pairs to filter by.

    Returns:
        list: A list of records that match the search criteria.
"""


def fetch_record_search(table_name, search_value=None, column_filters=None, column_in_filters=None, operand=None,
                        parent_call=None, range_filter=None, order_filter=None, result_card=None,
                        payload_data=None, module_id=None):
    conn = None
    cursor = None
    limit = None
    order_by = None
    order_direction = None
    range_start = None
    range_end = None
    resource_list = open_read_file('resources', '', 'general')
    row_id_column = resource_list['row_id_column']
    if operand:
        operand_value = operand
    else:
        operand_value = AND
    try:
        conn = get_connection()
        cursor = conn.cursor()
        if not search_value and not column_filters and not column_in_filters:
            query = sql.SQL("SELECT * FROM {}").format(
                sql.Identifier(table_name)
            )
            cursor.execute(query)
            records = cursor.fetchall()

            # Fetch column names
            column_names = [desc[0] for desc in cursor.description]

            # Filter out columns ending with "password"
            filtered_columns = [col for col in column_names if not col.endswith("password")]

            # Convert the records to a list of dictionaries excluding password columns
            records_list = [
                {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
                for record in records
            ]

            if records_list:
                return ResponseCode.create_response("SUCCESSFUL",
                                                    {"result": records_list, "record_length": len(records_list)})
            else:
                return ResponseCode.create_response("NO_DATA_FOUND")

        if range_filter and order_filter:
            # Unpack range_filter for start and end
            start = range_filter.get(resource_list['start_param'])
            record_size = range_filter.get(resource_list['record_size_param'])
            end = start + record_size
            limit = record_size
            # Unpack order_filter for ordering
            range_start = range_filter.get(resource_list['range_start_param'])
            range_end = range_filter.get(resource_list['range_end_param'])
            total_length = range_filter.get(resource_list['total_length_param'])
            order_by = order_filter.get(resource_list['order_by_param'], None)
            order_direction = order_filter.get(resource_list['order_direction_param'], "ASC").upper()

        # Get the columns of the table
        columns = get_schema_columns(table_name, cursor)

        # debug_print("columns:{}".format(columns))

        # Prepare the query to search across all columns or specific column-value pairs
        ilike_conditions = []
        filter_conditions = []
        parameters = []

        if search_value:
            # debug_print("search_value:{}".format(search_value))
            ilike_conditions = [
                sql.SQL("{}::text ILIKE %s").format(sql.Identifier(col)) for col in columns
            ]
            parameters.extend(["%" + search_value + "%"] * len(columns))

        if column_filters:
            # debug_print("column_filters:{}".format(column_filters))
            filter_conditions = [
                sql.SQL("{} = %s").format(sql.Identifier(col)) for col in column_filters.keys()
            ]
            parameters.extend(column_filters.values())

        if column_in_filters:
            # debug_print("column_in_filters:{}".format(column_in_filters))
            # if filter_conditions
            # 13/01/25 Modified to get the names instead of query id when returning the result
            if payload_data and not payload_data.get('create_search_cards'):
                filter_conditions.extend(
                    [sql.SQL("{} in %s").format(
                        sql.SQL("{0}.{1}").format(sql.Identifier('d'), sql.Identifier('product_id'))
                        if col == 'product_id' else sql.Identifier(col)
                    ) for col in column_in_filters.keys()]
                )
            else:
                # if filter_conditions
                filter_conditions.extend(
                    [sql.SQL("{} in %s").format(sql.Identifier(col)) for col in column_in_filters.keys()]
                )
            # debug_print("asdasdasd after:{}".format(sql.SQL("{0}.{1}").format(
            #     sql.Identifier('d'),
            #     sql.Identifier('product_id')
            # )))
            # debug_print("column_in_filters after:{}".format(filter_conditions))
            parameters.extend(column_in_filters.values())

        # Combine conditions
        combined_conditions = []
        if ilike_conditions:
            combined_conditions.append(sql.SQL("({})").format(sql.SQL(operand_value).join(ilike_conditions)))
        if filter_conditions:
            combined_conditions.extend(filter_conditions)

        debug_print("combined_conditions:{}".format(combined_conditions))

        # Prepare count query to get total records without limit/offset
        count_query = sql.SQL("SELECT count(*) FROM {} d WHERE ({})").format(
            sql.Identifier(table_name),
            sql.SQL(operand_value).join(combined_conditions)
        )

        # Prepare min/max rowid query (before applying LIMIT)
        rowid_range_query = sql.SQL("SELECT MIN(rowid), MAX(rowid) FROM {} d WHERE ({})").format(
            sql.Identifier(table_name),
            sql.SQL(operand_value).join(combined_conditions)
        )

        # Prepare the final query
        search_device_query = resource_list['search_device_query']
        # 13/01/25 Modified to get the names instead of query id when returning the result
        if payload_data and not payload_data.get('create_search_cards'):
            search_device_query_select = search_device_query.get(module_id + "_select")
            # debug_print("search_device_query: {}".format(search_device_query_select))
            query = sql.SQL(search_device_query_select + " AND ({})").format(
                sql.SQL(operand_value).join(combined_conditions)
            )
        else:
            query = sql.SQL("SELECT * FROM {} WHERE ({})").format(
                sql.Identifier(table_name),
                sql.SQL(operand_value).join(combined_conditions)
            )

        if parent_call:
            query += sql.SQL(" AND {} != False").format(sql.Identifier(parent_call))
            count_query += sql.SQL(" AND {} != False").format(sql.Identifier(parent_call))

        debug_print("rowid_range_query: {}".format(rowid_range_query))
        debug_print(cursor.mogrify(rowid_range_query, parameters).decode('utf-8'))
        cursor.execute(rowid_range_query, parameters)
        rowid_range = cursor.fetchone()
        min_rowid, max_rowid = rowid_range[0], rowid_range[1]

        # debug_print(f"Min RowID: {min_rowid}, Max RowID: {max_rowid}")

        if range_start is not None and range_end is not None:
            if order_direction == "ASC":
                query += sql.SQL(" AND rowid <= %s")
                #count_query += sql.SQL(" AND rowid <= %s")
                parameters.append(range_end)
            else:
                query += sql.SQL(" AND rowid <= %s")
                #count_query += sql.SQL(" AND rowid <= %s")
                parameters.append(range_start)

                # Execute rowid range query to get min(rowid) and max(rowid)

        # 13/01/25 Modified to get the names instead of query id when returning the result
        if payload_data and not payload_data.get('create_search_cards'):
            search_device_query_group_by = search_device_query.get(module_id + "_group_by")
            # debug_print("search_device_query_group_by : {}".format(search_device_query_group_by))
            query = query + sql.SQL(search_device_query_group_by)
            # debug_print("Final Query : {}".format(query))

        # Apply ordering if provided
        if order_by:
            query += sql.SQL(" ORDER BY {} {}").format(sql.Identifier(order_by), sql.SQL(order_direction))

        # Execute count query
        if total_length is not None:
            record_count = total_length
        else:
            cursor.execute(count_query, parameters)
            record_count = cursor.fetchone()[0]

        # Apply limit and offset if provided
        if limit is not None:
            query += sql.SQL(" LIMIT %s OFFSET %s")
            parameters.extend([limit, start])

        # debug_print(query.as_string(conn))  # Print the query for debugging
        # debug_print(parameters)  # Print the parameters for debugging

        cursor.execute(query, parameters)

        debug_print("in the else portion: {}".format(cursor.mogrify(query, parameters).decode('utf-8')))

        records = cursor.fetchall()
        # Fetch column names
        column_names = [desc[0] for desc in cursor.description]
        # Filter out columns ending with "password"
        filtered_columns = [col for col in column_names if not col.endswith("password")]

        # Convert records to a list of dictionaries excluding password columns
        records_list = []

        for idx, record in enumerate(records):
            record_dict = {col: record[i] for i, col in enumerate(column_names) if col in filtered_columns}
            records_list.append(record_dict)

        if order_direction == 'DESC':
            start_range = max_rowid  # In DESC, start is the maximum rowId
            end_range = min_rowid  # In DESC, end is the minimum rowId
        else:  # For ASC or any other cases, default to ascending logic
            start_range = min_rowid  # In ASC, start is the minimum rowId
            end_range = max_rowid  # In ASC, end is the maximum rowId

        if records_list:
            return ResponseCode.create_response("SUCCESSFUL", {
                "record_length": len(records_list), "total_length": record_count,
                "range_start": start_range,
                "range_end": end_range, "result": records_list, "result_card": result_card
            })
        else:
            return ResponseCode.create_response("NO_DATA_FOUND")
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("An error occurred: {}".format(str(e)))
        traceback.print_exc()
        raise e

    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)


def get_record_client(products: list):
    try:
        product_ids = []
        for product in products:
            product_ids.append(product['product_id'])
        return product_ids
    except Exception as e:
        debug_print("An error occurred: {}".format(str(e)))
        traceback.print_exc()
        raise e


def validate_payload_with_schema(data, table_name, cur=None):
    con = None
    is_new_cur = False
    try:
        if not cur:
            con = get_connection()
            cur = con.cursor()
            is_new_cur = True
        # print("validate payload calling....")
        """
        Validates the payload against the database schema.
        If a column is missing in the payload, it sets that column to None or a default value.
        """
        # Get all columns without default values from the table schema
        columns = get_schema_columns(table_name, cur)

        # Iterate through the columns and check if they are in the payload
        for column in columns:
            if column not in data:
                data[column] = None
        # debug_print("after set data to none : {}".format(data))
        return data
    except psycopg2.Error as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        return handle_database_exception(e)
    except Exception as e:
        traceback.print_exc()  # This will print the full traceback, including the line number
        debug_print("An error occurred: {}".format(str(e)))
        traceback.print_exc()
        raise e

    finally:
        if cur and is_new_cur:
            cur.close()
        if con and is_new_cur:
            release_connection(con)



"""
Function Name: update_based_rowid
Inputs:

Output: 
Description:
update_based_rowid
"""


def update_based_rowid(update_stmt, table_name, where_key_name, where_key_value, set_column_data_values):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Dynamically build the SET part of the query
        set_clauses = []
        values = []

        for col, val in set_column_data_values.items():
            set_clauses.append("{0} = %s".format(col))
            values.append(val)

        set_clause = ", ".join(set_clauses)

        # Prepare dynamic SQL safely
        query = update_stmt.format(TABLE_NAME=table_name, SET_CLAUSE=set_clause
                                   , WHERE_KEY_NAME=where_key_name, WHERE_KEY_VALUE=where_key_value)

        # debug_print("update query :  {}".format(query))
        # Execute
        cursor.execute(query, values)

        # debug_print(cursor.mogrify(query).decode('utf-8'))

        conn.commit()

    except psycopg2.Error:
        traceback.print_exc()
    except Exception as e:
        traceback.print_exc()
        logger.warning(str(e))  # str() to avoid Unicode issues
    finally:
        if cursor:
            cursor.close()
        if conn:
            release_connection(conn)
