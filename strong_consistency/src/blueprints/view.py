from flask import Blueprint
from shared_data import SharedData
from flask import request, jsonify

import util

view_api = Blueprint('view_api', __name__)

@view_api.route('/view', methods=['GET'])
def get_view():
    return jsonify(SharedData.current_view), 200


@view_api.route('/view', methods=['PUT'])
def update_view():
    data = request.get_json()
    if not data or "view" not in data:
        return jsonify(message='Request body must have "view" field.'), 400
    
    # Sort based on node ID and store value into variable.
    SharedData.current_view = sorted(data["view"], key=lambda x: x['id'])

    if not util.in_current_view():
        # Not in the new view => effectively do nothing
        SharedData.role = None
        return jsonify(message="Not in View"), 200

    if (util.is_primary()):
        SharedData.role = "primary"
    else:
        SharedData.role = "backup"
    
    return jsonify(message="View updated", role=SharedData.role), 200
