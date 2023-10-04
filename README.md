# GoogleCloud_Pyspark
Analyzing Taxi Cab Data  Dive deep into the world of taxi cab data with this repository, where we use PySpark on the Google Cloud Platform to uncover intriguing insights. Gauge the efficiency of taxi drivers. Compare payment modes - how often is cash used versus card? Determine average earnings of drivers. 

1 Description
The goal is to analyze a data set consisting of New York City Taxi trip reports in the Year 2013. The dataset was released under the FOIL (The Freedom of Information Law) and made public by Chris Whong
(https://chriswhong.com/open-data/foil_nyc_taxi/). 
2 Taxi Data set
The data set itself is a simple text file. Each taxi trip report is a different line in the file. Among other things, each trip report includes the starting point, the drop-off point, corresponding timestamps, and information related to the payment. The data are reported by the time that the trip ended, i.e., upon arrive in the order of the drop-off timestamps. The attributes present on each line of the file are in order as it was shown in table 1. The data files are in comma separated values (CSV) format. 
 
Table 1: Taxi data set fields.

The data files are in comma separated values (CSV) format. Example lines from the file are: 
07290D3599E7A0D62097A346EFCC1FB5,E7750A37CAB07D0DFF0AF7E3573AC141, 2013-01-01,00:00:00,2013-01-01 00:02:00,120,0.44,-73.956528,40.716976,-73.962440, 40.715008,CSH,3.50,0.50,0.50,0.00,0.00,4.50 
22D70BF00EEB0ADC83BA8177BB861991,3FF2709163DE7036FCAA4E5A3324E4BF, 2013-01-01,00:02:00,2013-01-01 00:02:00,0,0.00,0.000000,0.000000,0.000000,0.000000, CSH,27.00,0.00,0.50,0.00,0.00,27.50 
0EC22AAF491A8BD91F279350C2B010FD,778C92B26AE78A9EBDF96B49C67E4007, 2013-01-01,00:01:00,2013-01-01 00:03:00,120,0.71,-73.973145,40.752827,-73.965897 73.965897,40.760445,CSH,4.00,0.50,0.50,0.00,0.00,5.00 

# Use this code to pre-process the data in the csv file

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder.appName("TaxiData").getOrCreate()

import sys
input_path = sys.argv[1]

df = spark.read.csv("/content/taxi-data.csv", header=False, inferSchema=True)

def is_float(value):
  try:
    float(value)
    return True
  except ValueError:
    return False

from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

# Register the UDF with PySpark
is_float_udf = udf(is_float, BooleanType())

# Define conditions using the UDF and PySpark functions
corrected_df = df.filter(is_float_udf("_c5") & is_float_udf("_c11"))

# Show the cleaned DataFrame
corrected_df.show()

columns = ["medallion", "hack_license", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount", "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"]
renamed_columns = ["_c0", "_c1", "_c2", "_c3","_c4", "_c5", "_c6", "_c7","_c8", "_c9", "_c10", "_c11","_c12", "_c13", "_c14", "_c15","_c16"]

for orig_col, new_col in zip(renamed_columns, columns):
  corrected_df = corrected_df.withColumnRenamed(orig_col, new_col)

corrected_df.show()

# END

Questions:
1. Many different taxis have had multiple drivers. Write and execute a Python program that computes the top ten taxis that have had the largest number of drivers. Your output should be a set of (medallion, number of drivers) pairs.
Ans. Found in code "top_active_taxis_Pyspark.py"

2. We would like to figure out who the top 10 best drivers are in terms of their average earned money per minute spent carrying a customer. The total amount field is the total money earned on a trip. In the end, we are interested in computing a set of (driver, money per minute) pairs.
Ans. Found in code "top_drivers_pyspark.py"

3. We would like to know which hour of the day is the best time for drivers that has the highest profit per miles. Consider the surcharge amount in dollar for each taxi ride (without tip amount) and the distance in miles, and sum up the rides for each hour of the day (24 hours) – consider the pickup time for your calculation. The profit ratio is the ration surcharge in dollar divided by the travel distance in miles for each specific time of the day. 
   Profit Ratio = (Surcharge Amount in US Dollar) / (Travel Distance in miles) We are interested to know the time of the day that has the highest profit ratio. 
Ans. Found in code "best_time_pyspark.py"

4. What percentage of taxi customers pay with cash and what percentage use electronic cards? Analyze these payment methods for different time of the day and provide a list of percentages for each hour of the day? As a result provide two numbers for total percentages and a list like (hour of the day, percent paid card)
Ans. Found in "CashvsCard_pyspark.py"

5. We would like to measure the efficiency of taxis drivers by finding out their average earned money per mile. (Consider the total amount which includes tips, as their earned money) Implement a Spark job that can find out the top-10 efficient taxi divers. 
Ans. Found in code "efficient_drivers_pyspark.py"
