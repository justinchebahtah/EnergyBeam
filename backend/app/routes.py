from flask import Blueprint, jsonify, current_app
import json
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour, month, weekofyear, when, lag, avg, mean, trim
from pyspark.sql.window import Window
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import functions as F
import os

# Load AWS credentials from config file
config_path = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_path) as config_file:
    config = json.load(config_file)

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
    # Fetch the latest data from S3
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['aws_access_key_id'],
        aws_secret_access_key=config['aws_secret_access_key'],
        region_name=config['region']
    )
    obj = s3.get_object(
        Bucket='caisoscraperbucket0001', 
        Key='raw/20240524_20240524_SLD_FCST_ACTUAL_20240524_17_10_27_v1.csv'
    )
    df = pd.read_csv(obj['Body'])
    
    # Convert pandas DataFrame to Spark DataFrame
    spark = current_app.spark
    spark_df = spark.createDataFrame(df)
    
    # Trim whitespace from TAC_AREA_NAME (if necessary)
    spark_df = spark_df.withColumn("TAC_AREA_NAME", trim(spark_df["TAC_AREA_NAME"]))
    
    # Filter for CA ISO-TAC
    spark_df = spark_df.filter(spark_df["TAC_AREA_NAME"] == "CA ISO-TAC")
    
    # Add temporal features
    spark_df = spark_df.withColumn("Hour", hour(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("DayOfWeek", dayofweek(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("Month", month(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("WeekOfYear", weekofyear(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("IsWeekend", when(col("DayOfWeek").isin([1, 7]), 1).otherwise(0))
    
    # Define window partitioning
    window_spec = Window.partitionBy("TAC_AREA_NAME").orderBy("INTERVALSTARTTIME_GMT")
    
    # Add lag features
    spark_df = spark_df.withColumn("PrevHourLoad", lag(col("MW")).over(window_spec))
    spark_df = spark_df.withColumn("PrevDayLoad", lag(col("MW"), 24).over(window_spec))
    spark_df = spark_df.withColumn("PrevWeekLoad", lag(col("MW"), 24*7).over(window_spec))
    
    # Calculate moving averages
    spark_df = spark_df.withColumn("MovingAvg3Hours", avg(col("MW")).over(window_spec.rowsBetween(-3, 0)))
    spark_df = spark_df.withColumn("MovingAvg7Days", avg(col("MW")).over(window_spec.rowsBetween(-24*7, 0)))
    
    # Handle NULL values by imputation
    mean_prev_hour = spark_df.select(mean("PrevHourLoad")).collect()[0][0]
    mean_prev_day = spark_df.select(mean("PrevDayLoad")).collect()[0][0]
    mean_prev_week = spark_df.select(mean("PrevWeekLoad")).collect()[0][0]

    # Ensure the mean values are not None
    mean_prev_hour = mean_prev_hour if mean_prev_hour is not None else 0
    mean_prev_day = mean_prev_day if mean_prev_day is not None else 0
    mean_prev_week = mean_prev_week if mean_prev_week is not None else 0

    spark_df = spark_df.na.fill({"PrevHourLoad": mean_prev_hour, "PrevDayLoad": mean_prev_day, "PrevWeekLoad": mean_prev_week})

    # Prepare feature vector
    feature_columns = ["Hour", "DayOfWeek", "Month", "WeekOfYear", "IsWeekend", 
                       "PrevHourLoad", "PrevDayLoad", "PrevWeekLoad", "MovingAvg3Hours", "MovingAvg7Days"]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembled_df = assembler.transform(spark_df)
    
    # Scale the features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)
    
    # Load the GBTRegressionModel model
    model_path = 'models/caiso_GBM_model'
    model = GBTRegressionModel.load(model_path)
    
    # Make predictions
    predictions = model.transform(scaled_df)
    predictions = predictions.select('prediction').collect()
    predictions = [row['prediction'] for row in predictions]
    
    return jsonify(predictions)
