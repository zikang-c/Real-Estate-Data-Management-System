import streamlit as st
import base64
from backend_v3 import insert_property, search_property, update_property, delete_property
from PIL import Image

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


def image_to_base64(image_path):
    """
    Convert an image file to a base64 string.
    """
    with open(r"C:\Users\nguye\OneDrive\Desktop\A serene and luxurious scene capturing a rich life by a tropical beach.jpg", "rb") as img_file:
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
        uploaded_images = st.file_uploader("Upload Property Images", accept_multiple_files=True, type=ACCEPTED_IMAGE_TYPES)
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
    UI for searching properties.
    """
    st.subheader("🔍 Search for a Property")
    with st.form(key='search_property_form'):
        # # User selects the state from a dropdown
        # state = st.selectbox("Search by State", STATES_LIST)

        # User selects the search criteria (Custom ID, Address, or Type)
        search_by = st.radio("Search by", ["Custom ID", "Address", "Type"])

        if search_by == "Type":
            st.caption("For Type, please enter 'sale' or 'rent'. Case-insensitive.")
        elif search_by == "Address":
            st.caption("For Address, partial matches are allowed. Case-insensitive.")
        elif search_by == "Custom ID":
            st.caption("Custom ID requires exact match and case-sensitive.")

        # User enters the search value based on the selected criteria
        search_value = st.text_input("Enter Search Value (enter 'sale' or 'rent' for Type  ..||..  case-insensitive and partial match allowed for Address)")

        # User submits the search form
        submit_search = st.form_submit_button(label='Search Property')

        if submit_search:
            try:
                # Initialize an empty list to hold the results
                results = []

                # Call the appropriate backend function based on the selected search criteria
                if search_by == "Custom ID":
                    # Assuming there's a way to search by custom ID in your backend
                    results = search_property(custom_id=search_value)
                elif search_by == "Address":
                    results = search_property(address=search_value)
                else:  # Search by Type
                    results = search_property(property_type=search_value.lower())

                # Check if any results were found
                if results:
                    st.write("### Property Found:")
                    for result in results:
                        # Expander for Detailed Data
                        with st.expander(f"See details for {result.get('address', 'No Details')}"):
                            # You can choose to display the data as JSON or format it for better readability
                            st.json(result)
                else:
                    st.error("No property found with the given criteria.")
            except Exception as e:
                # Handle any errors that occur during the search
                st.error(f"An error occurred during the search: {e}")


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
    """
    Main function to display the UI components.
    """
    st.sidebar.title("🏠 Property Management")
    operation = st.sidebar.selectbox("Choose Operation", ["Add Property", "Search Property", "Update Property", "Delete Property"])

    if operation == "Add Property":
        add_property_ui()
    elif operation == "Search Property":
        search_property_ui()
    elif operation == "Update Property":
        update_property_ui()
    elif operation == "Delete Property":
        delete_property_ui()


if __name__ == "__main__":
    display_logo("path_to_your_logo_image.jpg")  # Specify the path to your logo image
    main()
