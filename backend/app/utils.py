import json
import os

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
from sqlalchemy import create_engine

""""
def get_db_connection():
    db_uri = os.getenv('SQLALCHEMY_DATABASE_URI')
    engine = create_engine(db_uri)
    return engine
"""


def get_db_connection():
    # Hardcoding the database URI for testing purposes
    db_uri = "postgresql://justinchebahtah@localhost/energybeam_test"
    engine = create_engine(db_uri)
    return engine


def fetch_latest_data_from_db():
    engine = get_db_connection()
    query = """
        SELECT * FROM caiso_load
        WHERE tac_area_name = 'CA ISO-TAC'
        ORDER BY interval_start_time DESC
        LIMIT 392
    """
    df = pd.read_sql(query, con=engine)
    df = df.drop(columns=["predicted"])  # Drop the 'predicted' column
    print(df.head())  # Print first few rows
    print(df.dtypes)  # Print data types of columns
    return df


def preprocess_data(spark, df):
    # Convert pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Trim and filter
    spark_df = spark_df.withColumn("tac_area_name", trim(spark_df["tac_area_name"]))

    # Filter for CA ISO-TAC
    spark_df = spark_df.filter(spark_df["tac_area_name"] == "CA ISO-TAC")

    # Add columns for hour, day of week, month, etc.
    spark_df = spark_df.withColumn("Hour", hour(col("interval_start_time")))
    spark_df = spark_df.withColumn("DayOfWeek", dayofweek(col("interval_start_time")))
    spark_df = spark_df.withColumn("Month", month(col("interval_start_time")))
    spark_df = spark_df.withColumn("WeekOfYear", weekofyear(col("interval_start_time")))
    spark_df = spark_df.withColumn(
        "IsWeekend", when(col("DayOfWeek").isin([1, 7]), 1).otherwise(0)
    )

    # Define window spec
    window_spec = Window.partitionBy("tac_area_name").orderBy("interval_start_time")

    # Add lag and moving average columns
    spark_df = spark_df.withColumn("PrevHourLoad", lag(col("mw")).over(window_spec))
    spark_df = spark_df.withColumn("PrevDayLoad", lag(col("mw"), 24).over(window_spec))
    spark_df = spark_df.withColumn(
        "PrevWeekLoad", lag(col("mw"), 24 * 7).over(window_spec)
    )
    spark_df = spark_df.withColumn(
        "MovingAvg3Hours", avg(col("mw")).over(window_spec.rowsBetween(-3, 0))
    )
    spark_df = spark_df.withColumn(
        "MovingAvg7Days", avg(col("mw")).over(window_spec.rowsBetween(-24 * 7, 0))
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

    # Check for Null median values
    if any(
        median is None
        for median in [
            median_prev_day,
            median_prev_week,
            median_moving_avg_3_hours,
            median_moving_avg_7_days,
        ]
    ):
        raise ValueError("One or more median values are None")

    # Fill PrevHourLoad with current value if it's NULL
    spark_df = spark_df.withColumn(
        "PrevHourLoad",
        when(spark_df["PrevHourLoad"].isNull(), spark_df["mw"]).otherwise(
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
