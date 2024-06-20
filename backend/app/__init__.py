from flask import Flask
from flask_cors import CORS
from pyspark.sql import SparkSession
import os

def create_app():
    app = Flask(__name__)

    # Load the configuration from the config.py file
    env = os.getenv('FLASK_CONFIG', 'development')
    app.config.from_object(f'config.{env.capitalize()}Config')

    # Enable CORS for all routes
    CORS(app)

    # Initialize Spark Session
    app.spark = SparkSession.builder.appName("EnergyBeam").getOrCreate()

    # Register blueprints
    from .routes import main
    app.register_blueprint(main)

    return app
