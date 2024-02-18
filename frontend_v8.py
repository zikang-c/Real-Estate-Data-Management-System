import streamlit as st
import base64
from backend_v8 import insert_property, search_property, filter_property, update_property, delete_property
from PIL import Image
import bcrypt
from pymongo import MongoClient


# Constants for the states list and file types for images
STATES_LIST = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
    "West Virginia", "Wisconsin", "Wyoming"
]
ACCEPTED_IMAGE_TYPES = ['jpg', 'png']


# MongoDB connection setup
with open('credentials.json') as f:
    data = json.load(f)
    MONGO_URI = data['url']
client = MongoClient(MONGO_URI)

# Database and collection names
db = client['authentication']
users_collection = db['login_info']


def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())


def insert_new_user(username, hashed_password):
    try:
        existing_user = users_collection.find_one({"username": username})
        if existing_user:
            st.error("Username already exists. Please choose a different username.")
            return False

        result = users_collection.insert_one({"username": username, "hashed_password": hashed_password})
        if result.inserted_id:
            return True
        else:
            st.error("Failed to insert new user.")
            return False
    except Exception as e:
        st.error(f"Exception occurred while registering user: {e}")
        return False


def login_ui():
    st.sidebar.subheader("Login")
    username = st.sidebar.text_input("Username", key="login_username")
    password = st.sidebar.text_input("Password", type="password", key="login_password")
    if st.sidebar.button("Login"):
        user_info = users_collection.find_one({"username": username})
        if user_info and bcrypt.checkpw(password.encode('utf-8'), user_info['hashed_password']):
            st.session_state["authenticated"] = True
            st.sidebar.success("You are logged in.")
            st.experimental_rerun()
        else:
            st.sidebar.error("Incorrect username or password.")


def registration_ui():
    st.sidebar.subheader("Register New Account")
    with st.sidebar.form("registration_form"):
        new_username = st.text_input("New Username", key="new_username_reg")
        new_password = st.text_input("New Password", type="password", key="new_password_reg")
        submit_button = st.form_submit_button("Register")

        if submit_button:
            if new_username and new_password:
                hashed_password = hash_password(new_password)
                if insert_new_user(new_username, hashed_password):
                    st.sidebar.success("User registered successfully.")
                else:
                    st.sidebar.error("Registration failed. Username might already exist.")
            else:
                st.sidebar.error("Username and password cannot be empty.")


                st.sidebar.error("Username and password cannot be empty.")


def image_to_base64(image_path):
    """
    Convert an image file to a base64 string.
    """
    with open(
            r"C:\Users\nguye\OneDrive\Desktop\A serene and luxurious scene capturing a rich life by a tropical beach.jpg",
            "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')


def display_logo(logo_path):
    """
    Display the company logo and title in the Streamlit app.
    """
    logo_base64 = image_to_base64(logo_path)
    logo_html = f"<img src='data:image/jpg;base64,{logo_base64}' class='img-fluid' width='150'>"
    st.markdown(f"""
    <div style="display: flex; align-items: center;">
        {logo_html}
        <h1 style="margin: 0 0 0 10px;">Majestic Real Estate Management</h1>
    </div>
    <div class="space"></div>

    """, unsafe_allow_html=True)


def convert_image_to_base64(uploaded_image):
    """
    Convert an uploaded image to a base64 string for storage.
    """
    buffer = uploaded_image.read()
    b64_encoded = base64.b64encode(buffer).decode()
    return f"data:image/{uploaded_image.type.split('/')[-1]};base64,{b64_encoded}"


def add_property_ui():
    """
    UI for adding a new property.
    """
    st.subheader("🏡 Add a New Property")
    with st.form(key='add_property_form'):
        col1, col2 = st.columns(2)
        with col1:
            address = st.text_input("Address")
            city = st.text_input("City")
            state = st.selectbox("State", STATES_LIST)
            zip_code = st.text_input("ZIP Code")
        with col2:
            price = st.number_input("Price ($)", min_value=0, value=150000, step=50000, format="%d")
            bedrooms = st.number_input("Bedrooms", min_value=0, value=3, step=1)
            bathrooms = st.number_input("Bathrooms", min_value=0.0, value=2.0, step=0.5)
            square_footage = st.number_input("Square Footage", min_value=0, value=1000, step=100)
        property_type = st.selectbox("Type", ["Sale", "Rent"])
        date_listed = st.date_input("Date Listed")
        description = st.text_area("Description")
        uploaded_images = st.file_uploader("Upload Property Images", accept_multiple_files=True,
                                           type=ACCEPTED_IMAGE_TYPES)
        submit_button = st.form_submit_button(label='Add Property')

        if submit_button:
            image_strings = [convert_image_to_base64(image) for image in uploaded_images] if uploaded_images else []
            property_data = {
                "address": address,
                "city": city,
                "state": state,
                "zip_code": int(zip_code) if zip_code.isdigit() else 0,
                "price": price,
                "bedrooms": bedrooms,
                "bathrooms": bathrooms,
                "square_footage": square_footage,
                "type": property_type.lower(),
                "date_listed": str(date_listed),
                "description": description,
                "images": image_strings
            }
            try:
                success = insert_property(property_data)
                if success:
                    st.success("Property added successfully!")
                else:
                    st.error("Failed to add property. Please check the input data.")
            except Exception as e:
                st.error(f"An error occurred: {e}")


def search_property_ui():
    """
    UI for searching properties based on basic criteria such as Custom ID, Address, or Type.
    Returns a list of properties that match the search criteria.

    :return: A list of dictionaries, each representing a property that matches the search criteria.
    """
    st.subheader("🔍 Search for a Property")
    search_results = []  # Initialize an empty list to store search results

    with st.form(key='search_property_form'):
        search_by = st.radio("Search by", ["Custom ID", "Address", "Type"])

        # Adjust captions based on the selected search criteria
        if search_by == "Type":
            st.caption("For Type, please enter 'sale' or 'rent'. Case-insensitive.")
        elif search_by == "Address":
            st.caption("For Address, partial matches are allowed. Case-insensitive.")
        elif search_by == "Custom ID":
            st.caption("Custom ID requires exact match and case-sensitive.")

        search_value = st.text_input("Enter Search Value")
        submit_search = st.form_submit_button(label='Search')

        if submit_search:
            try:
                if search_by == "Custom ID":
                    search_results = search_property(custom_id=search_value)
                elif search_by == "Address":
                    search_results = search_property(address=search_value)
                else:  # Search by Type
                    search_results = search_property(property_type=search_value.lower())

                # Optionally display search results here or handle display elsewhere
                if search_results:
                    st.write("### Search Results Found:")
                    for result in search_results:
                        with st.expander(f"See details for {result.get('address', 'No Details')}"):
                            st.json(result)  # Display the property details in JSON format
                            if result.get('images'):
                                for image_b64 in result['images']:
                                    st.image(image_b64, caption="Property Image", use_column_width=True)
                else:
                    st.error("No property found with the given criteria.")
            except Exception as e:
                st.error(f"An error occurred during the search: {e}")

    return search_results  # Return the list of search results


def filter_property_ui(properties):
    """
    UI for filtering properties based on advanced criteria such as price range, number of bedrooms, and bathrooms.

    :param properties: List of property dictionaries to be filtered.
    """
    st.subheader("🔍 Filter Properties")
    with st.form(key='filter_property_form'):
        # Advanced filters
        st.write("Specify Filter Criteria:")
        min_price = st.number_input("Minimum Price ($)", min_value=0, value=0, step=10000, format="%d")
        max_price = st.number_input("Maximum Price ($)", min_value=0, value=1000000, step=10000, format="%d")
        min_bedrooms = st.number_input("Minimum Bedrooms", min_value=0, value=0, step=1)
        max_bedrooms = st.number_input("Maximum Bedrooms", min_value=0, value=10, step=1)
        min_bathrooms = st.number_input("Minimum Bathrooms", min_value=0.0, value=0.0, step=0.5)
        max_bathrooms = st.number_input("Maximum Bathrooms", min_value=0.0, value=10.0, step=0.5)

        submit_filter = st.form_submit_button(label='Apply Filters')

        if submit_filter:
            # Apply filters to the provided properties list
            filtered_results = filter_property(
                properties,
                min_price=min_price,
                max_price=max_price,
                min_bedrooms=min_bedrooms,
                max_bedrooms=max_bedrooms,
                min_bathrooms=min_bathrooms,
                max_bathrooms=max_bathrooms
            )

            # Display filtered results
            if filtered_results:
                st.write("### Filtered Properties Found:")
                for result in filtered_results:
                    with st.expander(f"See details for {result.get('address', 'No Details')}"):
                        st.json(result)  # Display the property details in JSON format
                        if result.get('images'):
                            for image_b64 in result['images']:
                                st.image(image_b64, caption="Property Image", use_column_width=True)
            else:
                st.error("No properties found with the given filters.")


def update_property_ui():
    """
    UI for updating property details.
    """
    st.subheader("✏️ Update Property Details")
    with st.form(key='update_property_form'):
        custom_id = st.text_input("Property Custom ID")
        update_field = st.selectbox("Field to Update", ["Price", "Bedrooms", "Bathrooms", "Description"])
        new_value = st.text_input(f"New Value for {update_field}")
        submit_update = st.form_submit_button(label='Update Property')

        if submit_update:
            update_data = {update_field.lower(): new_value}
            success = update_property(custom_id, update_data)
            if success:
                st.success("Property updated successfully!")
            else:
                st.error("Failed to update property. Please check the Custom ID.")


def delete_property_ui():
    """
    UI for deleting a property.
    """
    st.subheader("🗑️ Delete a Property")
    with st.form(key='delete_property_form'):
        custom_id = st.text_input("Property Custom ID to Delete")
        submit_delete = st.form_submit_button(label='Delete Property')

        if submit_delete:
            confirm_delete = st.checkbox("I confirm that I want to delete this property", value=False)
            if confirm_delete:
                success = delete_property(custom_id)
                if success:
                    st.success("Property deleted successfully!")
                else:
                    st.error("Failed to delete property. Please check the Custom ID.")
            else:
                st.warning("Please confirm the deletion.")


def main():
    # Safely check if the user is authenticated, defaulting to False if the key doesn't exist
    is_authenticated = st.session_state.get("authenticated", False)

    if is_authenticated:
        # User is authenticated, show property management operations
        st.sidebar.title("🏠 Property Management")
        operation = st.sidebar.selectbox("Choose Operation",
                                         ["Add Property", "Search Property", "Update Property", "Delete Property"])

        if operation == "Add Property":
            add_property_ui()
        elif operation == "Search Property":
            search_results = search_property_ui()  # Modify this function to return search results
            st.session_state['search_results'] = search_results  # Store search results in session state
        elif operation == "Filter Properties":
            if 'search_results' in st.session_state:
                filter_property_ui(st.session_state['search_results'])  # Apply filters to the stored search results
            else:
                st.error("Please perform a search before applying filters.")
        elif operation == "Update Property":
            update_property_ui()
        elif operation == "Delete Property":
            delete_property_ui()
    else:
        # User is not authenticated, show login and optionally registration UI
        login_ui()

        registration_ui()


if __name__ == "__main__":
    main()
