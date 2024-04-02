import streamlit as st
import base64
from backend_v11 import insert_property, search_property, update_property, delete_property
from PIL import Image
import bcrypt
from io import BytesIO
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
MONGO_URI = 'mongodb+srv://nguyenlamvu88:Keepyou0ut99!!@cluster0.ymo3tge.mongodb.net/?retryWrites=true&w=majority'
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


def image_to_base64(image_path):
    """
    Convert an image file to a base64 string.
    """
    try:
        with open(image_path, "rb") as img_file:
            return base64.b64encode(img_file.read()).decode('utf-8')
    except Exception as e:
        st.error(f"Error reading image file: {e}")
        return None


def display_logo(logo_path):
    """
    Display the company logo and title in the Streamlit app.
    """
    logo_base64 = image_to_base64(logo_path)
    if logo_base64:
        logo_html = f"<img src='data:image/jpg;base64,{logo_base64}' class='img-fluid' width='350'>"
        st.markdown(f"""
            <div style="display: flex; align-items: center;">
                {logo_html}
                <h1 style="margin: 0 0 0 50px;">Majestic Real Estate Management</h1>
            </div>
            <div class="space"></div>
            """, unsafe_allow_html=True)
    else:
        st.error("Failed to display logo.")


def convert_image_to_base64(uploaded_image, size=(600, 400)):
    """
    Convert an uploaded image to a base64 string for storage.
    """
    try:
        # Extract file extension from filename and normalize to uppercase
        filename = uploaded_image.name
        file_extension = filename.split(".")[-1].lower()  # Ensure extension is in lowercase
        if file_extension not in ['jpg', 'png']:
            raise ValueError("Invalid file type")

        # Open the uploaded image with PIL
        image = Image.open(uploaded_image)

        # Resize the image
        resized_image = image.resize(size)

        # Save the resized image to a buffer, specifying format explicitly
        buffer = BytesIO()
        format = 'JPEG' if file_extension == 'jpg' else file_extension.upper()
        resized_image.save(buffer, format=format)  # Use explicit format
        buffer.seek(0)

        # Convert the image in the buffer to a base64 string
        b64_encoded = base64.b64encode(buffer.read()).decode()

        return f"data:image/{file_extension};base64,{b64_encoded}"
    except Exception as e:
        st.error(f"An error occurred while converting image to base64: {e}")
        return None


def display_image_in_base64(base64_string):
    st.markdown(
        f"<img src='{base64_string}' class='img-fluid'>", unsafe_allow_html=True
    )


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
    UI component for searching properties with optional sorting by price.
    """
    st.subheader("🔍 Search for Properties")
    # UI for search criteria
    with st.form("search_form"):
        city = st.text_input("City")
        st.caption("case-insensitive")  # Add a caption under the "City" input field

        state = st.text_input("State")
        st.caption("case-insensitive")  # Add a caption under the "State" input field

        property_type = st.text_input("Type")
        st.caption("sale or rent, case-insensitive")  # Add a caption under the "Type" input field

        address = st.text_input("Address")
        st.caption("partial match allowed, case-insensitive")  # Add a caption under the "Address" input field

        custom_id = st.text_input("Custom ID")
        sort_by_price = st.selectbox("Sort by Price", ["None", "Ascending", "Descending"], index=0)

        submit = st.form_submit_button("Search")

    if submit:
        # Translate UI option to sort parameter
        sort_option = None
        if sort_by_price == "Ascending":
            sort_option = 'asc'
        elif sort_by_price == "Descending":
            sort_option = 'desc'

        # Call the backend search function with sorting option
        search_results = search_property(city=city, state=state, property_type=property_type.lower(), address=address, custom_id=custom_id, sort_by_price=sort_option)

        # Filter duplicates based on custom_id
        unique_properties = {}
        for property in search_results:
            custom_id = property.get('custom_id')
            if custom_id not in unique_properties:
                unique_properties[custom_id] = property

        # Use the values from unique_properties, which are now unique
        unique_search_results = list(unique_properties.values())

        if unique_search_results:
            st.success(f"Found {len(unique_search_results)} unique properties.")
            for property in unique_search_results:
                st.json(property)  # Display property details as JSON

                # Display images if available, ensuring each image is unique
                images = list(set(property.get('images', [])))
                for img in images:
                    display_image_in_base64(img)
        else:
            st.warning("No properties found matching the criteria.")


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

    # Display logo after authentication
    display_logo(r"C:\Users\nguye\OneDrive\Desktop\homepage.jpg")

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
