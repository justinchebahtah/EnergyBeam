from flask import Blueprint, jsonify

# Create a blueprint
main = Blueprint('main', __name__)

# Define a test route
@main.route('/api')
def home():
    return jsonify({'message': 'Welcome to Energy Beam API'})

@main.route('/api/test')
def test():
    return jsonify({'message': 'Test endpoint working!'})
