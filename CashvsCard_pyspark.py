from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import hour, when
import sys

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: top_taxis.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]

    spark = SparkSession.builder.appName("Cash and Card").getOrCreate()

    is_float_udf = udf(is_float, BooleanType())

    df = spark.read.option("header", "false").csv(input_file)

    corrected_df = df.filter(is_float_udf("_c5") & is_float_udf("_c11"))

    columns = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs",
               "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude",
               "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]

    renamed_columns = ["_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7", "_c8", "_c9", "_c10", "_c11", "_c12", "_c13", "_c14", "_c15", "_c16"]

    for orig_col, new_col in zip(renamed_columns, columns):
        corrected_df = corrected_df.withColumnRenamed(orig_col, new_col)

    from pyspark.sql.functions import col
    cleaned_df = corrected_df.dropna()

    cleaned_df = cleaned_df.filter(col("medallion").isNotNull() & col("hack_license").isNotNull())

    df_with_payment_and_hour = cleaned_df.withColumn("payment_method", when(col("payment_type") == "CRD", "card").otherwise("cash")) \
                                .withColumn("pickup_hour", hour(col("pickup_datetime")))

    payment_method_percentages = df_with_payment_and_hour.groupBy("pickup_hour", "payment_method").count()
    total_counts = payment_method_percentages.groupBy("pickup_hour").agg({"count": "sum"}).withColumnRenamed("sum(count)", "total_count")

    payment_method_percentages = payment_method_percentages.join(total_counts, "pickup_hour") \
                                                            .withColumn("percentage", (col("count") / col("total_count")) * 100) \
                                                            .select("pickup_hour", "payment_method", "percentage")

    total_percentages = payment_method_percentages.groupBy("payment_method").agg({"percentage": "sum"}).withColumnRenamed("sum(percentage)", "total_percentage")

    hourly_payment_percentages = payment_method_percentages.filter(col("payment_method") == "card").select("pickup_hour", "percentage").collect()

    total_percentages = total_percentages.collect()

    for row in hourly_payment_percentages:
        print("(Hour: {}, Percent Paid with Card: {:.2f}%)".format(row["pickup_hour"], row["percentage"]))

    spark.stop()