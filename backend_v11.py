from pymongo import MongoClient, ASCENDING, DESCENDING
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
# The format to include time (e.g., "15-Mar-24 14:30") for exporting search results to json and csv files
formatted_dt = dt_now.strftime("%d-%b-%y %H:%M")

# Initialize logging
logging.basicConfig(level=logging.INFO)

# MongoDB connection
MONGO_URI = 'mongodb+srv://nguyenlamvu88:Keepyou0ut99!!@cluster0.ymo3tge.mongodb.net/?retryWrites=true&w=majority'
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
    The 'images' field is made optional, but if present, it should be a list.
    """
    optional_fields = ['images']  # Define which fields are optional

    errors = []  # List to accumulate error messages

    for key, expected_type in property_schema.items():
        # Check for missing fields
        if key not in property_data:
            if key not in optional_fields:  # Consider optional fields
                errors.append(f"Missing required field: '{key}'")
            continue

        # Ensure the field is of the correct type
        # Wrap expected_type in a tuple if it's not already one
        if not isinstance(expected_type, tuple):
            expected_type = (expected_type,)

        if not isinstance(property_data[key], expected_type):
            # Generate a friendly message for the expected types
            expected_types_formatted = ", ".join([t.__name__ for t in expected_type])
            errors.append(
                YELLOW + f"Field '{key}' is missing or has incorrect type. Expected {expected_types_formatted}, got {type(property_data[key]).__name__}" + RESET)

    # If there are any errors, raise an exception with all error messages
    if errors:
        error_message = "\n- " + "\n- ".join(errors)
        raise ValueError(error_message)


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
        logging.info(GREEN + f"\nProperty duplicated in {target_db_name} with same custom_id and _id: {result.inserted_id}" + RESET)
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
        logging.error(RED + f"\nValidation error: {ve}" + RESET)
        return False
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return False


def search_property(city=None, state=None, property_type=None, address=None, custom_id=None, sort_by_price=None):
    """
    Enhanced search function with optional sorting by price.

    :param city: Optional city where the property is located.
    :param state: Optional state where the property is located.
    :param property_type: Optional type of the property (e.g., 'sale', 'rent').
    :param address: Optional address of the property.
    :param custom_id: Optional custom ID of the property.
    :param sort_by_price: Optional sort direction ('asc' for ascending, 'desc' for descending).
    :return: A list of properties that match the search criteria, optionally sorted by price.
    """

    all_properties = {}
    query = {}

    if custom_id:
        query["custom_id"] = custom_id
    else:
        if city:
            query["city"] = {"$regex": city, "$options": "i"}
        if state:
            query["state"] = {"$regex": state, "$options": "i"}
        if property_type:
            query["type"] = {"$regex": property_type, "$options": "i"}
        if address:
            query["address"] = {"$regex": address, "$options": "i"}

    sort_order = [('price', ASCENDING)] if sort_by_price == 'asc' else [
        ('price', DESCENDING)] if sort_by_price == 'desc' else None

    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        results = properties_collection.find(query, sort=sort_order) if sort_order else properties_collection.find(
            query)

        for property in results:
            cid = property["custom_id"]
            if cid in all_properties:
                all_properties[cid]["source_db"].append(db_name)
            else:
                property["source_db"] = [db_name]
                all_properties[cid] = property

    return list(all_properties.values())


def export_to_csv(properties, filename=None):
    if filename is None:
        filename = f'search_results_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            if properties:
                writer = csv.DictWriter(file, fieldnames=properties[0].keys())
                writer.writeheader()
                for prop in properties:
                    writer.writerow(prop)
                print(GREEN + f"Results exported to CSV file: {filename}\n" + RESET)
            else:
                print(RED + "No properties to export.\n" + RESET)
    except Exception as e:
        logging.error(f"Error exporting to CSV: {e}")


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


def export_to_json(properties, filename=None):
    if filename is None:
        filename = f'search_results_{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            if properties:
                json.dump(properties, file, ensure_ascii=False, indent=4, cls=CustomEncoder)
                print(GREEN + f"Results exported to JSON file: {filename}\n" + RESET)
            else:
                print(RED + "No properties to export.\n" + RESET)
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
            logging.warning(YELLOW + f"Property with custom_id {custom_id} not found in {db_name}." + RESET)

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
            logging.info(GREEN + f"Property with custom_id {custom_id} deleted from {db_name}." + RESET)
            deletion_successful = True

        deletion_attempts += 1

    # Check if the deletion attempts were made across all databases
    if deletion_successful and deletion_attempts == len(DATABASE_NAMES):
        return True
    else:
        return False


def get_all_properties():
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        
        try:
            # Fetch all documents in the collection
            # properties = list(properties_collection.find({}))
            
            # exclude the '_id' field
            properties = list(properties_collection.find({}, {'_id': 0}))
            
            return properties
        except Exception as e:
            print(f"Failed to fetch properties: {e}")
            return []


def prompt_for_property_data():
    """
    Prompts the user for property details, collects the responses, and returns a property data dictionary.
    """
    print(BLUE + "Please enter the property details." + RESET)

    # List of fields to prompt for, with friendly display names and any special handling required
    fields = [
        ("address", "Address"),
        ("city", "City"),
        ("state", "State"),
        ("zip_code", "Zip Code", int),  # Special handling to convert to int
        ("price", "Price", float),  # Special handling to convert to float
        ("bedrooms", "Number of Bedrooms", int),
        ("bathrooms", "Number of Bathrooms", float),
        ("square_footage", "Square Footage", int),
        ("type", "Property Type (e.g., sale, rent)"),
        ("date_listed", "Date Listed (YYYY-MM-DD)"),
        ("description", "Property Description"),
        # 'images' is optional and user can input multiple images separated by commas
        ("images", "Images (optional, separate multiple paths with commas)", lambda x: x.split(',') if x else []),
    ]

    property_data = {}

    for field, prompt, *special_handling in fields:
        response = input(f"{prompt}: ").strip()
        # Apply any special handling for data conversion if necessary
        if special_handling:
            try:
                converter = special_handling[0]
                property_data[field] = converter(response)
            except ValueError:
                print(RED + f"\nError: Invalid input for '{prompt}'. Please ensure the input is correct and try again." + RESET)
                return None
        else:
            property_data[field] = response

    return property_data


def insert_property_interactive():
    """
    Handles the 'insert' operation in an interactive manner.
    """
    property_data = prompt_for_property_data()
    if property_data:
        success = insert_property(property_data)
        if success:
            print(GREEN + "\nProperty inserted successfully.\n" + RESET)
        else:
            print(RED + "\nFailed to insert property.\n" + RESET)
    else:
        print(YELLOW + "Property insertion cancelled due to invalid input." + RESET)


def search_property_interactive():
    print(BLUE + "Please enter search criteria (you can search by one or multiple criteria such as city, state, or address; hit enter to skip):" + RESET)
    city = input("City: ").strip()
    state = input("State: ").strip()
    property_type = input("Property Type (e.g., sale, rent): ").strip()
    address = input("Address: ").strip()
    custom_id = input("Custom ID: ").strip()
    sort_by_price = input("Sort by price (asc/desc, leave blank for no sorting): ").strip().lower()

    search_results = search_property(city=city, state=state, property_type=property_type, address=address, custom_id=custom_id, sort_by_price=sort_by_price)

    if search_results:
        print(GREEN + f"\nFound {len(search_results)} properties:\n" + RESET)
        for property in search_results:
            print_property(property)

        # Ask user if they want to export the results
        export_choice = input("Do you want to export the results? (yes/no): ").strip().lower()
        if export_choice == 'yes':
            # Ask for the format
            format_choice = input("Which format to export? (csv/json): ").strip().lower()
            if format_choice == 'csv':
                export_to_csv(search_results)
            elif format_choice == 'json':
                export_to_json(search_results)
            else:
                print(RED + "Invalid format selected. Export cancelled." + RESET)
    else:
        print(RED + "\nNo properties found matching the criteria.\n" + RESET)


def print_property(property_info):
    print(BLUE + "------" + RESET)
    print(f"Custom ID: {property_info.get('custom_id', 'N/A')}")
    print(f"Address: {property_info.get('address', 'N/A')}")
    print(f"City: {property_info.get('city', 'N/A')}")
    print(f"State: {property_info.get('state', 'N/A')}")
    print(f"Zip Code: {property_info.get('zip_code', 'N/A')}")
    print(f"Price: ${property_info.get('price', 'N/A')}")
    print(f"Bedrooms: {property_info.get('bedrooms', 'N/A')}")
    print(f"Bathrooms: {property_info.get('bathrooms', 'N/A')}")
    print(f"Square Footage: {property_info.get('square_footage', 'N/A')}")
    print(f"Type: {property_info.get('type', 'N/A')}")
    print(f"Date Listed: {property_info.get('date_listed', 'N/A')}")
    print(f"Description: {property_info.get('description', 'N/A')}")
    if 'images' in property_info and property_info['images']:
        print("Images:")
        for image in property_info['images']:
            print(f"  - {image}")
    else:
        print("Images: N/A")
    dbs = property_info.get('source_db', [])
    print(YELLOW + f"Found in database: {', '.join(dbs)}" + RESET)
    print(BLUE + "------\n" + RESET)


def update_property_interactive():
    custom_id = input(BLUE + "Please enter the Custom ID of the property to update: " + RESET).strip()
    if not custom_id:
        print(YELLOW + "\nCustom ID is required to update a property.\n" + RESET)
        return

    print(BLUE + "Enter the updates for the property (hit enter to skip):" + RESET)
    updates = {}
    # Here, list all properties that can be updated. Just a few examples are provided.
    updates['address'] = input("New Address (leave blank if no change): ").strip() or None
    updates['price'] = input("New Price (leave blank if no change): ").strip() or None
    updates['description'] = input("New Description (leave blank if no change): ").strip() or None

    # Filter out the fields where no change was made (i.e., input was left blank)
    updates = {k: v for k, v in updates.items() if v is not None}

    if updates:
        success = update_property(custom_id, updates)
        if success:
            print(GREEN + "\nProperty updated successfully.\n" + RESET)
        else:
            print(RED + "\nFailed to update property. The custom_id is incorrect or not found.\n" + RESET)
    else:
        print(YELLOW + "\nNo updates were made.\n" + RESET)


def delete_property_interactive():
    custom_id = input(BLUE + "Please enter the Custom ID of the property to delete: " + RESET).strip()
    if custom_id:
        success = delete_property(custom_id)
        if success:
            print(GREEN + "\nProperty deleted successfully.\n" + GREEN)
        else:
            print(RED + "\nFailed to delete property. The custom_id is incorrect or not found\n" + RESET)
    else:
        print(YELLOW + "\nCustom ID is required to delete a property.\n" + RESET)


def main():
    parser = argparse.ArgumentParser(description="Property Management System")
    parser.add_argument('--operation', choices=['insert', 'search', 'update', 'delete', 'interactive_insert', 'interactive_search', 'interactive_delete', 'interactive_update'], required=True,
                        help="Operation to perform: insert, search, update, delete, interactive_insert, interactive_search, interactive_delete, or interactive_update")

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
    parser.add_argument('--images', nargs='*', help="List of property images")
    parser.add_argument('--init', action='store_true', help="Initialize database indexes")

    args = parser.parse_args()

    if args.init:
        initialize_indexes()
        print(GREEN + "\n\nDatabase indexes initialized successfully.\n" + RESET)

    if args.operation == 'interactive_insert':
        insert_property_interactive()

    elif args.operation == 'interactive_search':
        search_property_interactive()

    elif args.operation == 'interactive_delete':
        delete_property_interactive()

    elif args.operation == 'interactive_update':
        update_property_interactive()

    elif args.operation == 'insert':
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
            print(GREEN + "\nProperty inserted successfully.\n" + RESET)
        else:
            print(RED + "\nFailed to insert property.\n" + RESET)

    elif args.operation == 'search':
        search_results = search_property(city=args.city, state=args.state, property_type=args.type,
                                         address=args.address, custom_id=args.custom_id)

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
                print(GREEN + "\nProperty updated successfully.\n" + RESET)
            else:
                print(RED + "\nFailed to update property.\n" + RESET)
        else:
            print(YELLOW + "\nCustom ID and updates are required for update operation.\n" + RESET)

    elif args.operation == 'delete':
        if args.custom_id:
            success = delete_property(args.custom_id)
            if success:
                print(GREEN + "\nProperty deleted successfully.\n" + RESET)
            else:
                print(RED + "Failed to delete property.\n" + RESET)
        else:
            print(YELLOW + "\nCustom ID is required for delete operation.\n" + RESET)


if __name__ == "__main__":
    main()

"""
Interactive Command-line Interface Instructions:

Initializes database indexes:
python backend_v11.py --init

Inserting a Property: Follow the prompts for each property detail
  python backend_v11.py --operation interactive_insert

Searching for Properties: Follow the prompts for search criteria
  python backend_v11.py --operation interactive_search

Updating a Property: Follow the prompts to specify the property and updates
  python backend_v11.py --operation interactive_update

Deleting a Property: Follow the prompt to enter the Custom ID of the property to delete
  python backend_v11.py --operation interactive_delete
"""


"""
Single Command-Line Interface Instructions:

Initializes database indexes:
python backend_v11.py --init

Inserting a Property: provide details in accordance with the property schema
python backend_v11.py --operation insert --address "123 Main St" --city "New York" --state "New York" --zip_code 10001 --price 1500000 --bedrooms 3 --bathrooms 2.5 --square_footage 2000 --type "sale" --date_listed "2024-02-15" --description "Spacious family home" --images "image1.jpg" "image2.jpg" "image3.jpg"

Searching for Properties: use any combination of city, state, type, and address
python backend_v11.py --operation search --city "New York" --type "rent"
python backend_v11.py --operation search --custom_id "STA-TOWN-123"

Updating a Property: need to provide its custom ID and the updates in a field=value format
python backend_v11.py --operation update --custom_id "NY-NYC-123" --updates "price=2500" 

Deleting a Property: provide its custom ID
python backend_v11.py --operation delete --custom_id "NY-NYC-123"

"""
