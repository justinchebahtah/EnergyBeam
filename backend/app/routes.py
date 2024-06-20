import boto3
from flask import Blueprint, current_app, jsonify
from datetime import datetime, timedelta

from .utils import (
    fetch_latest_data_from_db,
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
    # Fetch the latest data from DB
    df = fetch_latest_data_from_db()

    # Convert pandas DataFrame to Spark DataFrame and preprocess
    spark = current_app.spark
    spark_df = preprocess_data(spark, df)

    # Get the latest 24 hours of data
    latest_time_str = spark_df.agg({"interval_start_time": "max"}).collect()[0][0]

    # Checks if INTERVALSTARTTIME_GMT is in 'yyyy-MM-ddTHH:mm:ss-00:00' format
    if isinstance(latest_time_str, datetime):
        latest_time = latest_time_str
    else:
        latest_time = datetime.strptime(latest_time_str, '%Y-%m-%dT%H:%M:%S%z')
    start_time = latest_time - timedelta(hours=24)

    recent_df = spark_df.filter(spark_df["interval_start_time"] > start_time.strftime('%Y-%m-%dT%H:%M:%S%z'))

    # Collect data to Python list
    recent_data = recent_df.select("interval_start_time", "MW").orderBy("interval_start_time").collect()
    response_data = [{"time": row["interval_start_time"], "MW": row["MW"]} for row in recent_data]

    return jsonify(response_data)

@main.route("/api/predict")
def predict():
    # Fetch the latest data from DB
    df = fetch_latest_data_from_db()

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
