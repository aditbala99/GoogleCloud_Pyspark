from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import col
from pyspark.sql.functions import hour, col
import sys

# Define UDF to check if a string can be converted to float
def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: top_taxis.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    spark = SparkSession.builder.appName("Best Hour of the Day").getOrCreate()

    # Register the UDF with PySpark
    is_float_udf = udf(is_float, BooleanType())

    # Read the input CSV file
    df = spark.read.option("header", "false").csv(input_file)

    # Apply filtering using the UDF and PySpark functions
    corrected_df = df.filter(is_float_udf("_c5") & is_float_udf("_c11"))

    # Rename columns
    columns = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs",
               "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
               "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]

    renamed_columns = ["_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16"]

    for orig_col, new_col in zip(renamed_columns, columns):
        corrected_df = corrected_df.withColumnRenamed(orig_col, new_col)
    
    cleaned_df = corrected_df.dropna()
      
    cleaned_df = cleaned_df.filter(col("medallion").isNotNull() & col("hack_license").isNotNull())
    
    df_with_profit_ratio = cleaned_df.withColumn("profit_ratio", col("surcharge") / col("trip_distance"))
    
    df_with_hour = df_with_profit_ratio.withColumn("pickup_hour", hour(col("pickup_datetime")))
    
    average_profit_ratio_by_hour = df_with_hour.groupBy("pickup_hour").agg({"profit_ratio": "avg"}).withColumnRenamed("avg(profit_ratio)", "avg_profit_ratio")
    
    best_hour = average_profit_ratio_by_hour.orderBy(col("avg_profit_ratio").desc()).limit(1)
    
    best_hour.write.mode("overwrite").csv(output_file, header=True)
    
    best_hour.show()
    
    spark.stop()
    