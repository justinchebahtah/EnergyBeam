from flask import Flask
from flask_cors import CORS
from pyspark.sql import SparkSession

def create_app():
    app = Flask(__name__)
    
    # Enable CORS for all routes
    CORS(app)
    
    # Initialize Spark Session
    app.spark = SparkSession.builder \
        .appName("EnergyBeam") \
        .getOrCreate()
    
    # Register blueprints
    from .routes import main
    app.register_blueprint(main)
    
    return app
