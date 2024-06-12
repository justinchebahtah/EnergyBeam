import boto3
from flask import Blueprint, current_app, jsonify
from datetime import datetime, timedelta

from .utils import (
    fetch_latest_data_from_s3,
    load_aws_credentials,
    load_model,
    make_predictions,
    prepare_features,
    preprocess_data,
)

# Create a blueprint
main = Blueprint("main", __name__)


# Define a test route
@main.route("/api")
def home():
    return jsonify({"message": "Welcome to Energy Beam API"})


@main.route("/api/display-today")
def display_today():
    # Load AWS credentials
    config = load_aws_credentials()

    # Fetch the latest data from S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config["region"],
    )
    df = fetch_latest_data_from_s3(
        s3,
        "caisoscraperbucket0001",
        "raw/combined_CAISO_load_demand_data_weekly_catchup.csv",
    )

    # Convert pandas DataFrame to Spark DataFrame and preprocess
    spark = current_app.spark
    spark_df = preprocess_data(spark, df)

    # Get the latest 24 hours of data
    latest_time_str = spark_df.agg({"INTERVALSTARTTIME_GMT": "max"}).collect()[0][0]
    
    # Assuming the INTERVALSTARTTIME_GMT is in 'yyyy-MM-ddTHH:mm:ss-00:00' format
    latest_time = datetime.strptime(latest_time_str, '%Y-%m-%dT%H:%M:%S%z')
    start_time = latest_time - timedelta(hours=24)
    
    recent_df = spark_df.filter(spark_df["INTERVALSTARTTIME_GMT"] > start_time.strftime('%Y-%m-%dT%H:%M:%S%z'))

    # Collect data to Python list
    recent_data = recent_df.select("INTERVALSTARTTIME_GMT", "MW").orderBy("INTERVALSTARTTIME_GMT").collect()
    response_data = [{"time": row["INTERVALSTARTTIME_GMT"], "MW": row["MW"]} for row in recent_data]

    return jsonify(response_data)


@main.route("/api/predict")
def predict():
    # Load AWS credentials
    config = load_aws_credentials()

    # Fetch the latest data from S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config["region"],
    )
    df = fetch_latest_data_from_s3(
        s3,
        "caisoscraperbucket0001",
        "raw/combined_CAISO_load_demand_data_weekly_catchup.csv",
    )

    # Convert pandas DataFrame to Spark DataFrame and preprocess
    spark = current_app.spark
    spark_df = preprocess_data(spark, df)

    # Prepare features
    scaled_df = prepare_features(spark_df)

    # Load the GBTRegressionModel model
    model = load_model("models/caiso_GBM_model")

    # Make predictions
    predictions = make_predictions(model, scaled_df)

    return jsonify(predictions)
