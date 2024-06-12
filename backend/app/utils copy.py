import json
import os

import pandas as pd
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressionModel
from pyspark.sql.functions import percentile_approx
from pyspark.sql.functions import (
    avg,
    col,
    dayofweek,
    hour,
    lag,
    mean,
    month,
    trim,
    weekofyear,
    when,
)
from pyspark.sql.window import Window


def load_aws_credentials():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path) as config_file:
        config = json.load(config_file)
    return config


def fetch_latest_data_from_s3(s3_client, bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj["Body"])
    return df


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
    
    # Calculate median values for imputation
    median_prev_hour = spark_df.select(percentile_approx("PrevHourLoad", 0.5)).collect()[0][0]
    median_prev_day = spark_df.select(percentile_approx("PrevDayLoad", 0.5)).collect()[0][0]
    median_prev_week = spark_df.select(percentile_approx("PrevWeekLoad", 0.5)).collect()[0][0]
    median_moving_avg_3_hours = spark_df.select(percentile_approx("MovingAvg3Hours", 0.5)).collect()[0][0]
    median_moving_avg_7_days = spark_df.select(percentile_approx("MovingAvg7Days", 0.5)).collect()[0][0]

    # Fill PrevHourLoad with current value if it's null
    spark_df = spark_df.withColumn("PrevHourLoad", 
                                   when(spark_df["PrevHourLoad"].isNull(), spark_df["MW"]).otherwise(spark_df["PrevHourLoad"]))
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
    spark_df.select("PrevHourLoad", "PrevDayLoad", "PrevWeekLoad", "MovingAvg3Hours", "MovingAvg7Days").show(20)

    
    """
    # Fill null values with median values
    spark_df = spark_df.na.fill({
        "PrevHourLoad": median_prev_hour,
        "PrevDayLoad": median_prev_day,
        "PrevWeekLoad": median_prev_week,
        "MovingAvg3Hours": median_moving_avg_3_hours,
        "MovingAvg7Days": median_moving_avg_7_days
    })
    
     # Print median values for sanity check
    print("Median PrevHourLoad:", median_prev_hour)
    print("Median PrevDayLoad:", median_prev_day)
    print("Median PrevWeekLoad:", median_prev_week)
    print("Median MovingAvg3Hours:", median_moving_avg_3_hours)
    print("Median MovingAvg7Days:", median_moving_avg_7_days)
    """
    
    """
     # Fill missing values with the current hour's load
    spark_df = spark_df.withColumn(
        "PrevHourLoad", when(col("PrevHourLoad").isNull(), col("MW")).otherwise(col("PrevHourLoad"))
    )
    spark_df = spark_df.withColumn(
        "PrevDayLoad", when(col("PrevDayLoad").isNull(), col("MW")).otherwise(col("PrevDayLoad"))
    )
    spark_df = spark_df.withColumn(
        "PrevWeekLoad", when(col("PrevWeekLoad").isNull(), col("MW")).otherwise(col("PrevWeekLoad"))
    )
    spark_df = spark_df.withColumn(
        "MovingAvg3Hours", when(col("MovingAvg3Hours").isNull(), col("MW")).otherwise(col("MovingAvg3Hours"))
    )
    spark_df = spark_df.withColumn(
        "MovingAvg7Days", when(col("MovingAvg7Days").isNull(), col("MW")).otherwise(col("MovingAvg7Days"))
    )
    """
    """
    mean_prev_hour = spark_df.select(mean("PrevHourLoad")).collect()[0][0]
    mean_prev_day = spark_df.select(mean("PrevDayLoad")).collect()[0][0]
    mean_prev_week = spark_df.select(mean("PrevWeekLoad")).collect()[0][0]

    mean_prev_hour = mean_prev_hour if mean_prev_hour is not None else 0
    mean_prev_day = mean_prev_day if mean_prev_day is not None else 0
    mean_prev_week = mean_prev_week if mean_prev_week is not None else 0
    

    spark_df = spark_df.na.fill(
        {
            "PrevHourLoad": mean_prev_hour,
            "PrevDayLoad": mean_prev_day,
            "PrevWeekLoad": mean_prev_week,
        }
    )
    """

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
