import json
import os

from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    dayofweek,
    hour,
    lag,
    month,
    percentile_approx,
    trim,
    weekofyear,
    when,
)
from pyspark.sql.window import Window


def load_db_credentials():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config


def get_db_connection():
    config = load_db_credentials()
    engine = create_engine(f"postgresql://{config['db_user']}:{config['db_password']}@{config['db_host']}/{config['db_name']}")
    return engine


def fetch_latest_data_from_db():
    engine = get_db_connection()
    query = "SELECT * FROM caiso_load_demand ORDER BY INTERVALSTARTTIME_GMT DESC LIMIT 24"
    df = pd.read_sql(query, con=engine)
    return df

"""
def load_aws_credentials():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config


def fetch_latest_data_from_s3(s3_client, bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
    return df
"""

def preprocess_data(spark, df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.withColumn("TAC_AREA_NAME", trim(spark_df["TAC_AREA_NAME"]))
    spark_df = spark_df.filter(spark_df["TAC_AREA_NAME"] == "CA ISO-TAC")
    spark_df = spark_df.withColumn("Hour", hour(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("DayOfWeek", dayofweek(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("Month", month(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn(
        "WeekOfYear", weekofyear(col("INTERVALSTARTTIME_GMT"))
    )
    spark_df = spark_df.withColumn(
        "IsWeekend", when(col("DayOfWeek").isin([1, 7]), 1).otherwise(0)
    )

    window_spec = Window.partitionBy("TAC_AREA_NAME").orderBy("INTERVALSTARTTIME_GMT")
    spark_df = spark_df.withColumn("PrevHourLoad", lag(col("MW")).over(window_spec))
    spark_df = spark_df.withColumn("PrevDayLoad", lag(col("MW"), 24).over(window_spec))
    spark_df = spark_df.withColumn(
        "PrevWeekLoad", lag(col("MW"), 24 * 7).over(window_spec)
    )
    spark_df = spark_df.withColumn(
        "MovingAvg3Hours", avg(col("MW")).over(window_spec.rowsBetween(-3, 0))
    )
    spark_df = spark_df.withColumn(
        "MovingAvg7Days", avg(col("MW")).over(window_spec.rowsBetween(-24 * 7, 0))
    )

    # Calculate median values for NULL value replacement
    median_prev_day = spark_df.select(percentile_approx("PrevDayLoad", 0.5)).collect()[
        0
    ][0]
    median_prev_week = spark_df.select(
        percentile_approx("PrevWeekLoad", 0.5)
    ).collect()[0][0]
    median_moving_avg_3_hours = spark_df.select(
        percentile_approx("MovingAvg3Hours", 0.5)
    ).collect()[0][0]
    median_moving_avg_7_days = spark_df.select(
        percentile_approx("MovingAvg7Days", 0.5)
    ).collect()[0][0]

    # Fill PrevHourLoad with current value if it's NULL
    spark_df = spark_df.withColumn(
        "PrevHourLoad",
        when(spark_df["PrevHourLoad"].isNull(), spark_df["MW"]).otherwise(
            spark_df["PrevHourLoad"]
        ),
    )
    # Fill the rest with medians
    spark_df = spark_df.na.fill(
        {
            "PrevDayLoad": median_prev_day,
            "PrevWeekLoad": median_prev_week,
            "MovingAvg3Hours": median_moving_avg_3_hours,
            "MovingAvg7Days": median_moving_avg_7_days,
        }
    )

    # Print the median values for sanity check
    print(f"Median PrevDayLoad: {median_prev_day}")
    print(f"Median PrevWeekLoad: {median_prev_week}")
    print(f"Median MovingAvg3Hours: {median_moving_avg_3_hours}")
    print(f"Median MovingAvg7Days: {median_moving_avg_7_days}")

    # Print the filled values for sanity check
    spark_df.select(
        "PrevHourLoad",
        "PrevDayLoad",
        "PrevWeekLoad",
        "MovingAvg3Hours",
        "MovingAvg7Days",
    ).show(20)

    return spark_df


def prepare_features(spark_df):
    feature_columns = [
        "Hour",
        "DayOfWeek",
        "Month",
        "WeekOfYear",
        "IsWeekend",
        "PrevHourLoad",
        "PrevDayLoad",
        "PrevWeekLoad",
        "MovingAvg3Hours",
        "MovingAvg7Days",
    ]
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembled_df = assembler.transform(spark_df)

    scaler = StandardScaler(
        inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True
    )
    scaler_model = scaler.fit(assembled_df)
    scaled_df = scaler_model.transform(assembled_df)
    return scaled_df


def load_model(model_path):
    model = GBTRegressionModel.load(model_path)
    return model


def make_predictions(model, scaled_df):
    predictions = model.transform(scaled_df)
    predictions = predictions.select("prediction").collect()
    predictions = [row["prediction"] for row in predictions]
    return predictions


def preprocess_data_with_custom_prev_hour_load(spark, df, prev_hour_load_value):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.withColumn("TAC_AREA_NAME", trim(spark_df["TAC_AREA_NAME"]))
    spark_df = spark_df.filter(spark_df["TAC_AREA_NAME"] == "CA ISO-TAC")
    spark_df = spark_df.withColumn("Hour", hour(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("DayOfWeek", dayofweek(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn("Month", month(col("INTERVALSTARTTIME_GMT")))
    spark_df = spark_df.withColumn(
        "WeekOfYear", weekofyear(col("INTERVALSTARTTIME_GMT"))
    )
    spark_df = spark_df.withColumn(
        "IsWeekend", when(col("DayOfWeek").isin([1, 7]), 1).otherwise(0)
    )

    window_spec = Window.partitionBy("TAC_AREA_NAME").orderBy("INTERVALSTARTTIME_GMT")
    spark_df = spark_df.withColumn("PrevHourLoad", lag(col("MW")).over(window_spec))
    spark_df = spark_df.withColumn("PrevDayLoad", lag(col("MW"), 24).over(window_spec))
    spark_df = spark_df.withColumn(
        "PrevWeekLoad", lag(col("MW"), 24 * 7).over(window_spec)
    )
    spark_df = spark_df.withColumn(
        "MovingAvg3Hours", avg(col("MW")).over(window_spec.rowsBetween(-3, 0))
    )
    spark_df = spark_df.withColumn(
        "MovingAvg7Days", avg(col("MW")).over(window_spec.rowsBetween(-24 * 7, 0))
    )

    median_prev_day = spark_df.select(percentile_approx("PrevDayLoad", 0.5)).collect()[
        0
    ][0]
    median_prev_week = spark_df.select(
        percentile_approx("PrevWeekLoad", 0.5)
    ).collect()[0][0]
    median_moving_avg_3_hours = spark_df.select(
        percentile_approx("MovingAvg3Hours", 0.5)
    ).collect()[0][0]
    median_moving_avg_7_days = spark_df.select(
        percentile_approx("MovingAvg7Days", 0.5)
    ).collect()[0][0]

    spark_df = spark_df.withColumn(
        "PrevHourLoad",
        when(spark_df["PrevHourLoad"].isNull(), prev_hour_load_value).otherwise(
            spark_df["PrevHourLoad"]
        ),
    )
    spark_df = spark_df.na.fill(
        {
            "PrevDayLoad": median_prev_day,
            "PrevWeekLoad": median_prev_week,
            "MovingAvg3Hours": median_moving_avg_3_hours,
            "MovingAvg7Days": median_moving_avg_7_days,
        }
    )

    return spark_df



