"""
mongoTableEntityOperation.py
==============
Author: Stanley Parmar
Description: Module to handle database operations including creating, updating, and deleting records.
"""

# tableEntityOperation.py

from dbMongoConnection import MongoDBConnection


def pagination_and_sort_field(cursor, sort_fields, limit, offset):
    # Apply sorting if provided
    if sort_fields:
        sort_list = []

        # loop over field name and order(default order Desc)
        for field in sort_fields:
            if ":" in field:
                field_name, direction = field.split(":", 1)
                sort_order = 1 if direction.lower() == "asc" else -1
            else:
                field_name = field
                sort_order = -1
            sort_list.append((field_name.strip(), sort_order))
        
        cursor = cursor.sort(sort_list)

    # Apply offset (skip)
    if offset:
        cursor = cursor.skip(offset)

    # Apply limit
    if limit:
        cursor = cursor.limit(limit)

    return cursor


def create_record(collection_name, data):
    mongo = None
    try:
        # Connect to MongoDB
        mongo = MongoDBConnection()

        # Get collection
        collection = mongo.get_mongo_collection(collection_name)

        # Insert single or multiple records
        if isinstance(data, list):
            result = collection.insert_many(data)
            return result.inserted_ids
        else:
            result = collection.insert_one(data)
            return result.inserted_id

    except Exception as e:
        raise e
    finally:
        if mongo:
            mongo.close()


"""
sort_fields should be a list of tuples: [("field", 1 or -1)]
limit is the max number of documents
offset is the number of documents to skip (skip in MongoDB)
cursor is chainable, so methods can be applied in any order
"""

def fetch_record(collection, selected_fields=None, criteria=None, sort_fields=None, limit=None, offset=None):
    mongo = None
    try:
        # mongo db connection
        mongo = MongoDBConnection()

        # get mongo collection
        collection = mongo.get_mongo_collection(collection)
        query_filter = {}

        # Fetch Data Based On criteria
        if criteria:

            # Dynamic Filter method.
            for key, value in criteria.items():
                if isinstance(value, list):
                    query_filter[key] = {"$in": value}
                else:
                    query_filter[key] = value

        # selected Field From Collection
        projection = (
            {field: 1 for field in selected_fields if not field.endswith("password")}
            if selected_fields else None
        )

        # Fetch Data From collection
        cursor = collection.find(query_filter, projection)

        # pagination and sort field
        cursor = pagination_and_sort_field(cursor, sort_fields, limit, offset)

        # Convert to list here before closing connection
        return list(cursor)

    except Exception as e:
        pass
        # debug_print("Failed to fetch records: {}".format(e))
        raise e
    finally:
        if mongo:
            mongo.close()


# def create_mongo_collection(collection_name):
#     mongo = None
#     try:
#         # mongo db connection
#         mongo = MongoDBConnection()

#         # create Mongo collection
#         mongo.db.createCollection(collection_name)

#         # create Collection Successfully

#     except Exception as e:
#         pass
#         # debug_print("Failed to fetch records: {}".format(e))
#         raise e
#     finally:
#         if mongo:
#             mongo.close()