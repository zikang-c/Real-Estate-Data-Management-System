from pymongo import MongoClient
import logging
import hashlib
import argparse
import sys
import json
from bson import ObjectId
import csv
import datetime


# ANSI escape codes for text colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"  # Reset to default color


# Initialize logging
logging.basicConfig(level=logging.INFO)

dt_now = datetime.datetime.now()
formatted_dt = dt_now.strftime("%d-%b-%y")


def check_connection(client):
    """
    Check the MongoDB connection by attempting to retrieve server information.
    If the connection is unsuccessful, an exception will be raised.
    """
    try:
        # Attempt to retrieve server information as a connection check
        client.server_info()
        logging.info(BLUE + "\nSuccessfully connected to MongoDB." + RESET)
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        sys.exit("Exiting due to unsuccessful MongoDB connection.")


# MongoDB connection
MONGO_URI = 'mongodb+srv://nguyenlamvu88:Keepyou0ut99!!@cluster0.ymo3tge.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(MONGO_URI)

check_connection(client)

# Database names
DATABASE_NAMES = ['properties_db1', 'properties_db2', 'properties_db3', 'properties_db4']

# Property schema for validation
property_schema = {
    "address": str,
    "city": str,
    "state": str,
    "zip_code": int,
    "price": (int, float),
    "bedrooms": int,
    "bathrooms": float,
    "square_footage": int,
    "type": str,
    "date_listed": str,
    "description": str,
    "images": list
}


def initialize_indexes():
    """
    Create indexes on commonly queried fields across all databases to enhance query performance.
    """
    # Fields to be indexed
    index_fields = ['city', 'state', 'type', 'address', 'custom_id']

    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']

        for field in index_fields:
            try:
                # Create an index on each field, if it doesn't already exist
                properties_collection.create_index([(field, 1)])  # 1 for ascending order
                logging.info(f"Index on '{field}' created in {db_name}.")

            except Exception as e:
                logging.error(f"Failed to create index on '{field}' in {db_name}: {e}")


"""
Custom ID Creation: create_custom_id function generates a unique ID using state, city, and address.

Hash Function: generate_hash computes an integer hash value from the custom ID.

Database Selection: get_database selects one of the four databases based on the hash value.

Data Validation: validate_property_data ensures the input data conforms to the property_schema.

Existence Check: property_already_exists verifies if a property with the same custom ID is already present in any of the databases.

Data Insertion: insert_property handles the insertion process, including all the above steps.
"""


def create_custom_id(state, city, address):
    """
    Generate a custom ID using the state, city, and address.
    """
    state_abbr = state[:3].upper()
    city_abbr = city[:4].upper()
    address_num = ''.join(filter(str.isdigit, address))
    return f"{state_abbr}-{city_abbr}-{address_num}"


def generate_hash(custom_id):
    """
    Generate an integer hash value from the custom_id.
    """
    hash_obj = hashlib.sha256(custom_id.encode())
    return int(hash_obj.hexdigest(), 16)


def get_database(custom_id):
    """
    Select a database based on the hash of the custom_id.
    """
    hash_value = generate_hash(custom_id)
    db_index = hash_value % len(DATABASE_NAMES)
    return client[DATABASE_NAMES[db_index]]


def validate_property_data(property_data):
    """
    Validate property data against the defined schema.
    """
    for key, expected_type in property_schema.items():
        if key not in property_data:
            raise ValueError(f"Missing required field: {key}")
        if not isinstance(property_data[key], expected_type):
            if not isinstance(property_data[key], tuple(expected_type)):
                raise TypeError(f"Field {key} has incorrect type. Expected {expected_type}, got {type(property_data[key])}")


def property_already_exists(custom_id):
    """
    Check across all databases if a property with the given custom_id already exists.
    """
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        if db['properties'].find_one({"custom_id": custom_id}):
            return True
    return False


def insert_property(property_data):
    """
    Insert a property into the appropriate database based on custom_id hash.
    """
    try:
        validate_property_data(property_data)

        custom_id = create_custom_id(property_data['state'], property_data['city'], property_data['address'])
        if property_already_exists(custom_id):
            raise ValueError(RED + f"\nProperty with custom_id {custom_id} already exists." + RESET)

        property_data['custom_id'] = custom_id

        db = get_database(custom_id)
        properties_collection = db['properties']

        result = properties_collection.insert_one(property_data)
        logging.info(GREEN + f"\nProperty inserted in {db.name} with custom_id: {custom_id} and _id: {result.inserted_id}" + RESET)

        return True
    except ValueError as ve:
        logging.error(f"Validation error: {ve}")
        return False
    except TypeError as te:
        logging.error(f"Type error: {te}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False


def search_property(city=None, state=None, property_type=None, address=None, custom_id=None):
    """
    Search for properties based on optional criteria: city, state, type, address, or custom_id.
    The search will return properties matching any of the provided criteria.

    :param city: Optional city where the property is located.
    :param state: Optional state where the property is located.
    :param property_type: Optional type of the property (e.g., 'sale', 'rent').
    :param address: Optional address of the property.
    :param custom_id: Optional custom ID of the property.
    :return: A list of properties that match any of the provided criteria.
    """
    matching_properties = []  # List to store matching properties from all databases
    query = {}

    # Construct the query based on provided criteria using $or operator
    or_query = []
    if city:
        or_query.append({"city": city})
    if state:
        or_query.append({"state": state})
    if property_type:
        or_query.append({"type": property_type})
    if address:
        or_query.append({"address": {"$regex": address, "$options": "i"}})  # Case-insensitive partial match
    if custom_id:
        or_query.append({"custom_id": custom_id})

    if or_query:
        query["$or"] = or_query

    # Iterate over each database and query for properties
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']

        # Query for properties in the current database
        results = properties_collection.find(query)

        # Add the results from the current database to the aggregate list
        matching_properties.extend(list(results))

    return matching_properties


# Export search results to a csv or json file
def export_to_csv(properties, filename=f'search_results {formatted_dt}.csv'):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=properties[0].keys())
        writer.writeheader()
        for property in properties:
            writer.writerow(property)
    print(YELLOW + f"Results exported to a csv file: {filename}\n" + RESET)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


def export_to_json(properties, filename=f'search_results {formatted_dt}.json'):
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(properties, file, ensure_ascii=False, indent=4, cls=CustomEncoder)
        print(YELLOW + f"Results exported to a json file: {filename}\n" + RESET)
    except Exception as e:
        logging.error(f"Error exporting to JSON: {e}")


def update_property(custom_id, updates):
    """
    Update a property identified by custom_id with the provided updates.

    :param custom_id: The unique identifier for the property.
    :param updates: A dictionary containing the fields to update and their new values.
    :return: A boolean indicating success or failure of the update operation.
    """
    success = False

    # Iterate over each database to find and update the property
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']

        result = properties_collection.update_one({"custom_id": custom_id}, {"$set": updates})

        if result.matched_count > 0:  # Check if the property was found and updated
            logging.info(GREEN + f"\nProperty with custom_id {custom_id} updated in {db_name}." + RESET)
            success = True
            break  # Exit loop once the property is updated

    if not success:
        logging.warning(f"No property found with custom_id {custom_id} to update.")

    return success


def delete_property(custom_id):
    """
    Delete a property identified by custom_id.

    :param custom_id: The unique identifier for the property to be deleted.
    :return: A boolean indicating success or failure of the delete operation.
    """
    success = False

    # Iterate over each database to find and delete the property
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']

        result = properties_collection.delete_one({"custom_id": custom_id})

        if result.deleted_count > 0:  # Check if the property was found and deleted
            logging.info(f"Property with custom_id {custom_id} deleted from {db_name}.")
            success = True
            break  # Exit loop once the property is deleted

    if not success:
        logging.warning(f"No property found with custom_id {custom_id} to delete.")

    return success


def main():
    parser = argparse.ArgumentParser(description="Property Management System")
    parser.add_argument('--operation', choices=['insert', 'search', 'update', 'delete'], required=True,
                        help="Operation to perform: insert, search, update, delete")
    parser.add_argument('--city', help="City where the property is located")
    parser.add_argument('--state', help="State where the property is located")
    parser.add_argument('--type', help="Type of the property (e.g., 'sale', 'rent')")
    parser.add_argument('--address', help="Address of the property")
    parser.add_argument('--custom_id', help="Custom ID of the property")
    parser.add_argument('--updates', nargs='*', help="Updates to apply in the format: field1=value1 field2=value2")
    parser.add_argument('--zip_code', type=int, help="Zip code of the property")
    parser.add_argument('--price', type=float, help="Price of the property")
    parser.add_argument('--bedrooms', type=int, help="Number of bedrooms")
    parser.add_argument('--bathrooms', type=float, help="Number of bathrooms")
    parser.add_argument('--square_footage', type=int, help="Square footage of the property")
    parser.add_argument('--date_listed', help="Date when the property was listed")
    parser.add_argument('--description', help="Description of the property")
    parser.add_argument('--images', nargs='+', help="List of property images")
    parser.add_argument('--init', action='store_true', help="Initialize database indexes")

    args = parser.parse_args()

    if args.init:
        initialize_indexes()
        print(GREEN + "\n\nDatabase indexes initialized successfully.\n" + RESET)

    if args.operation == 'insert':
        property_data = {
            "address": args.address,
            "city": args.city,
            "state": args.state,
            "zip_code": args.zip_code,
            "price": args.price,
            "bedrooms": args.bedrooms,
            "bathrooms": args.bathrooms,
            "square_footage": args.square_footage,
            "type": args.type,
            "date_listed": args.date_listed,
            "description": args.description,
            "images": args.images
        }
        success = insert_property(property_data)
        if success:
            print(GREEN + "\n\nProperty inserted successfully.\n" + RESET)
        else:
            print(RED + "\n\nFailed to insert property.\n" + RESET)

    elif args.operation == 'search':
        search_results = search_property(city=args.city, state=args.state, property_type=args.type, address=args.address, custom_id=args.custom_id)

        if search_results:
            print(GREEN + f"\nFound {len(search_results)} properties:\n" + RESET)
            for property in search_results:
                print(property, '\n')
            export_to_csv(search_results)
            export_to_json(search_results)
        else:
            print(YELLOW + "No properties found matching the criteria.\n" + RESET)

    elif args.operation == 'update':
        if args.custom_id and args.updates:
            updates = {u.split('=')[0]: u.split('=')[1] for u in args.updates}
            success = update_property(args.custom_id, updates)
            if success:
                print(GREEN + "\n\nProperty updated successfully.\n" + RESET)
            else:
                print(RED + "\n\nFailed to update property.\n" + RESET)
        else:
            print(YELLOW + "\n\nCustom ID and updates are required for update operation.\n" + RESET)

    elif args.operation == 'delete':
        if args.custom_id:
            success = delete_property(args.custom_id)
            if success:
                print(GREEN + "\n\nProperty deleted successfully.\n" + RESET)
            else:
                print(RED + "Failed to delete property.\n" + RESET)
        else:
            print(YELLOW + "\n\nCustom ID is required for delete operation.\n" + RESET)


if __name__ == "__main__":
    main()

"""
command-line interface Instructions:

Initializes database indexes:
python backend_v3.py --init

Inserting a Property: provide details in accordance with the property schema
python backend_v3.py --operation insert --address "123 Main St" --city "New York" --state "NY" --zip_code 10001 --price 1500000 --bedrooms 3 --bathrooms 2.5 --square_footage 2000 --type "sale" --date_listed "2024-02-15" --description "Spacious family home" --images "image1.jpg" "image2.jpg" "image3.jpg"


Searching for Properties: use any combination of city, state, type, and address
python backend_v3.py --operation search --city "New York" --type "rent"
python backend_v3.py --operation search --custom_id "STA-TOWN-123"

Updating a Property: need to provide its custom ID and the updates in a field=value format
python backend_v3.py --operation update --custom_id "NY-NYC-123" --updates "price=2500" "type=sale"

Deleting a Property: provide its custom ID
python backend_v3.py --operation delete --custom_id "NY-NYC-123"

"""
