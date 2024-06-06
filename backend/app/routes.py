from flask import Blueprint, jsonify, current_app
from .utils import load_aws_credentials, fetch_latest_data_from_s3, preprocess_data, prepare_features, load_model, make_predictions
import boto3

# Create a blueprint
main = Blueprint('main', __name__)

# Define a test route
@main.route('/api')
def home():
    return jsonify({'message': 'Welcome to Energy Beam API'})

@main.route('/api/test')
def test():
    return jsonify({'message': 'Test endpoint working!'})

@main.route('/api/predict')
def predict():
    # Load AWS credentials
    config = load_aws_credentials()

    # Fetch the latest data from S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'],
        region_name=config['region']
    )
    df = fetch_latest_data_from_s3(s3, 'caisoscraperbucket0001', 'raw/20240524_20240524_SLD_FCST_ACTUAL_20240524_17_10_27_v1.csv')
    
    # Convert pandas DataFrame to Spark DataFrame and preprocess
    spark = current_app.spark
    spark_df = preprocess_data(spark, df)
    
    # Prepare features
    scaled_df = prepare_features(spark_df)
    
    # Load the GBTRegressionModel model
    model = load_model('models/caiso_GBM_model')
    
    # Make predictions
    predictions = make_predictions(model, scaled_df)
    
    return jsonify(predictions)
