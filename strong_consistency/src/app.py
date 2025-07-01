# app.py
import os

from shared_data import SharedData
from flask import Flask, request, jsonify

app = Flask(__name__)

# Register all blueprints into the API. 
from blueprints.data import data_api
app.register_blueprint(data_api)

from blueprints.view import view_api
app.register_blueprint(view_api)


@app.route('/ping', methods=['GET'])
def ping():
    return jsonify(message=f'Node {SharedData.NODE_IDENTIFIER} is up.'), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081)
