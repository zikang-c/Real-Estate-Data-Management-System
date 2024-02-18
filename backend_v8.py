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

dt_now = datetime.datetime.now()
formatted_dt = dt_now.strftime("%d-%b-%y")

# Initialize logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection
with open('credentials.json') as f:
    data = json.load(f)
    MONGO_URI = data['url']
client = MongoClient(MONGO_URI)

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


def check_connection():
    """
    Check the MongoDB connection by attempting to retrieve server information.
    """
    try:
        client.server_info()
        logging.info(BLUE + "\nSuccessfully connected to MongoDB." + RESET)
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        sys.exit("Exiting due to unsuccessful MongoDB connection.")


def initialize_indexes():
    """
    Create indexes on commonly queried fields across all databases to enhance query performance.
    """
    index_fields = ['city', 'state', 'type', 'address', 'custom_id']
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        for field in index_fields:
            properties_collection.create_index([(field, 1)])
            logging.info(f"Index on '{field}' created in {db_name}.")


def create_custom_id(state, city, address):
    """
    Generate a custom ID using the state, city, and address.
    The city name will have all whitespace removed before using in the ID.
    """
    state_abbr = state[:3].upper().strip()
    city_abbr = ''.join(city.split())[:4].upper()  # Remove all whitespace and then take the first 4 characters
    address_num = ''.join(filter(str.isdigit, address))

    custom_id = f"{state_abbr}-{city_abbr}-{address_num}"
    return custom_id


def get_database(custom_id):
    """
    Select a database based on the hash of the custom_id.
    """
    hash_obj = hashlib.sha256(custom_id.encode())
    hash_value = int(hash_obj.hexdigest(), 16)
    db_index = hash_value % len(DATABASE_NAMES)
    return client[DATABASE_NAMES[db_index]]


def generate_hash_for_duplication(custom_id, exclude_db):
    """
    Generate a hash to decide the target database for duplication, excluding the original database.
    """
    hash_obj = hashlib.sha256(custom_id.encode())
    hash_value = int(hash_obj.hexdigest(), 16)

    # Use a different modulus operation or logic to select the target database for duplication
    target_db_index = hash_value % (len(DATABASE_NAMES) - 1)  # Exclude the original database

    # Adjust the index if the calculated index is equal to or greater than the index of the excluded database
    if DATABASE_NAMES.index(exclude_db) <= target_db_index:
        target_db_index += 1

    return DATABASE_NAMES[target_db_index]


def validate_property_data(property_data):
    """
    Validate property data against the defined schema.
    """
    for key, expected_type in property_schema.items():
        if key not in property_data:
            raise ValueError(f"Missing required field: {key}")
        if not isinstance(property_data[key], expected_type) and not isinstance(property_data[key],
                                                                                tuple(expected_type)):
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


def duplicate_property(property_data, target_db_name):
    """
    Duplicate the property data into the target database.
    """
    try:
        db = client[target_db_name]
        properties_collection = db['properties']
        result = properties_collection.insert_one(property_data)
        logging.info(GREEN + f"\nProperty duplicated in {target_db_name} with _id: {result.inserted_id}" + RESET)
        return True
    except Exception as e:
        logging.error(f"Failed to duplicate property in {target_db_name}: {e}")
        return False


def insert_property(property_data):
    """
    Insert a property into the appropriate database based on custom_id hash and duplicate it into one other database.
    """
    try:
        validate_property_data(property_data)

        custom_id = create_custom_id(property_data['state'], property_data['city'], property_data['address'])
        if property_already_exists(custom_id):
            raise ValueError(RED + f"Property with custom_id {custom_id} already exists." + RESET)

        property_data['custom_id'] = custom_id

        # Original insertion
        original_db = get_database(custom_id)
        properties_collection = original_db['properties']
        result = properties_collection.insert_one(property_data)
        logging.info(GREEN + f"\nProperty inserted in {original_db.name} with custom_id: {custom_id} and _id: {result.inserted_id}" + RESET)

        # Determine the target database for duplication
        target_db_name = generate_hash_for_duplication(custom_id, original_db.name)
        # Perform the duplication
        duplicate_property(property_data, target_db_name)

        return True
    except ValueError as ve:
        logging.error(f"Validation error: {ve}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False


def filter_property(properties, state=None, type=None, min_price=None, max_price=None, min_bedrooms=None,
                    max_bedrooms=None, min_bathrooms=None, max_bathrooms=None):
    """
    Filters a list of property dictionaries based on specified criteria including state and type.

    :param properties: List of property dictionaries to filter.
    :param state: Specific state to filter properties by.
    :param type: Specific type of property to filter by (e.g., 'sale', 'rent').
    :param min_price: Minimum price for filtering properties.
    :param max_price: Maximum price for filtering properties.
    :param min_bedrooms: Minimum number of bedrooms for filtering properties.
    :param max_bedrooms: Maximum number of bedrooms for filtering properties.
    :param min_bathrooms: Minimum number of bathrooms for filtering properties.
    :param max_bathrooms: Maximum number of bathrooms for filtering properties.
    :return: A list of filtered property dictionaries.
    """
    filtered_properties = []

    for property in properties:
        if state is not None and property.get('state', '').lower() != state.lower():
            continue
        if type is not None and property.get('type', '').lower() != type.lower():
            continue
        if min_price is not None and property.get('price', 0) < min_price:
            continue
        if max_price is not None and property.get('price', 0) > max_price:
            continue
        if min_bedrooms is not None and property.get('bedrooms', 0) < min_bedrooms:
            continue
        if max_bedrooms is not None and property.get('bedrooms', 0) > max_bedrooms:
            continue
        if min_bathrooms is not None and property.get('bathrooms', 0) < min_bathrooms:
            continue
        if max_bathrooms is not None and property.get('bathrooms', 0) > max_bathrooms:
            continue

        filtered_properties.append(property)

    return filtered_properties


def search_property(city=None, state=None, property_type=None, address=None, custom_id=None):
    """
    Search for properties based on optional criteria: city, state, type, address, or custom_id.
    Each property in the search results will include the 'source_db' field indicating the database it came from.
    The search will return properties matching any of the provided criteria.

    :param city: Optional city where the property is located.
    :param state: Optional state where the property is located.
    :param property_type: Optional type of the property (e.g., 'sale', 'rent').
    :param address: Optional address of the property.
    :param custom_id: Optional custom ID of the property.
    :return: A list of properties that match any of the provided criteria, with an additional 'source_db' field for each property.
    """

    matching_properties = []
    query = {}

    # Construct the query based on the provided criteria
    if custom_id:
        query = {"custom_id": custom_id}
    else:
        and_query = []
        if city: and_query.append({"city": city})
        if state: and_query.append({"state": state})
        if property_type: and_query.append({"type": property_type})
        if address: and_query.append({"address": {"$regex": address, "$options": "i"}})  # Case-insensitive partial match
        if and_query: query["$and"] = and_query

    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        try:
            # Execute the query and retrieve results from the current database
            results = properties_collection.find(query)
            # Add the 'source_db' field to each property and append it to the list of matching properties
            for property in results:
                property_with_source = dict(property)  # Convert the property to a dict if it's not already one
                property_with_source['source_db'] = db_name  # Add the 'source_db' field
                matching_properties.append(property_with_source)
        except Exception as e:
            print(f"Error querying {db_name}: {e}")

    # Use the '_id' field to ensure distinct results, preserving the 'source_db' information
    unique_properties = {prop['_id']: prop for prop in matching_properties}.values()
    return list(unique_properties)


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
    Update a property identified by custom_id with the provided updates in all replicated databases.

    :param custom_id: The unique identifier for the property.
    :param updates: A dictionary containing the fields to update and their new values.
    :return: A boolean indicating overall success or failure of the update operation.
    """
    update_successful = False
    update_attempts = 0

    # Iterate over each database to update the property
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        result = properties_collection.update_one({"custom_id": custom_id}, {"$set": updates})

        if result.matched_count > 0:
            logging.info(GREEN + f"Property with custom_id {custom_id} updated in {db_name}." + RESET)
            update_successful = True
        else:
            logging.warning(f"Property with custom_id {custom_id} not found in {db_name}.")

        update_attempts += 1

    # Check if the update attempts were made across all databases
    if update_successful and update_attempts == len(DATABASE_NAMES):
        return True
    else:
        return False


def delete_property(custom_id):
    """
    Delete a property identified by custom_id from all replicated databases.

    :param custom_id: The unique identifier for the property to be deleted.
    :return: A boolean indicating overall success or failure of the delete operation.
    """
    deletion_successful = False
    deletion_attempts = 0

    # Iterate over each database to delete the property
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        result = properties_collection.delete_one({"custom_id": custom_id})

        if result.deleted_count > 0:
            logging.info(f"Property with custom_id {custom_id} deleted from {db_name}.")
            deletion_successful = True
        else:
            logging.warning(f"Property with custom_id {custom_id} not found in {db_name}.")

        deletion_attempts += 1

    # Check if the deletion attempts were made across all databases
    if deletion_successful and deletion_attempts == len(DATABASE_NAMES):
        return True
    else:
        return False


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
python backend_v8.py --init

Inserting a Property: provide details in accordance with the property schema
python backend_v8.py --operation insert --address "123 Main St" --city "New York" --state "NY" --zip_code 10001 --price 1500000 --bedrooms 3 --bathrooms 2.5 --square_footage 2000 --type "sale" --date_listed "2024-02-15" --description "Spacious family home" --images "image1.jpg" "image2.jpg" "image3.jpg"

Searching for Properties: use any combination of city, state, type, and address
python backend_v8.py --operation search --city "New York" --type "rent"
python backend_v8.py --operation search --custom_id "STA-TOWN-123"

Updating a Property: need to provide its custom ID and the updates in a field=value format
python backend_v8.py --operation update --custom_id "NY-NYC-123" --updates "price=2500" "type=sale"

Deleting a Property: provide its custom ID
python backend_v8.py --operation delete --custom_id "NY-NYC-123"

"""
