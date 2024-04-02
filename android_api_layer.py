from flask import Flask, request, jsonify
from bson import ObjectId
from pymongo import MongoClient
import backend_v11 
# from backend_v11 import search_property

app = Flask(__name__)

# MongoDB connection
MONGO_URI = 'mongodb+srv://nguyenlamvu88:Keepyou0ut99!!@cluster0.ymo3tge.mongodb.net/?retryWrites=true&w=majority'
client = MongoClient(MONGO_URI)

# Database names
DATABASE_NAMES = ['properties_db1', 'properties_db2', 'properties_db3', 'properties_db4']


@app.route('/insert', methods=['POST'])
def insert_property():
    property_data = request.json
    success = backend_v11.insert_property(property_data)
    return jsonify({"success": success})

def ObjectIdToStr(o):
    if isinstance(o, ObjectId):
        return str(o)
    raise TypeError("Object of type ObjectId is not JSON serializable")

@app.route('/search', methods=['GET'])
def search_properties():
    query = request.args.get('query')

    all_properties = []
    
    for db_name in DATABASE_NAMES:
        db = client[db_name]
        properties_collection = db['properties']
        results = properties_collection.find({
            "$or": [
                {"city": query},
                {"state": query},
                {"property_type": query},
                {"address": query},
                {"custom_id": query}
            ]
        })
        all_properties.extend(list(results))

    return jsonify(all_properties)
   

@app.route('/update', methods=['POST'])
def update_property():
    custom_id = request.json.get('custom_id')
    updates = request.json.get('updates')
    success = backend_v11.update_property(custom_id, updates)
    return jsonify({"success": success})


@app.route('/delete', methods=['DELETE'])
def delete_property():
    custom_id = request.args.get('custom_id')
    success = backend_v11.delete_property(custom_id)
    return jsonify({"success": success})


@app.route('/properties', methods=['GET'])
def get_properties():
    try:
        properties = backend_v11.get_all_properties()
        return jsonify(properties)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
