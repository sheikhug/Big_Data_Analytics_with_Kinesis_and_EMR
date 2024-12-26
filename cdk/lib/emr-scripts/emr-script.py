import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# Function to parse command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Process raw data from S3 and perform analysis.")
    
    parser.add_argument('--raw_data_bucket', type=str, required=True, 
                        help='The S3 bucket containing the raw data.')
    
    parser.add_argument('--processed_data_bucket', type=str, required=True, 
                        help='The S3 bucket containing the processed data.')
    
    parser.add_argument('--analysis_prefix', type=str, required=True, 
                        help='The prefix or directory within the raw data bucket where analysis results will be saved.')
    
    return parser.parse_args()

args = parse_args()

spark = SparkSession.builder.appName("EMR-DataAnalysis").getOrCreate()

raw_data_path = f"s3://{args.raw_data_bucket}"

df = spark.read.json(raw_data_path)

df.printSchema()

# Perform basic analysis
# 1. Calculate the average trip duration
avg_trip_duration = df.agg(avg("tripDuration").alias("avg_trip_duration")).collect()[0]["avg_trip_duration"]

# 2. Calculate the average passenger count
avg_passenger_count = df.agg(avg("passengerCount").alias("avg_passenger_count")).collect()[0]["avg_passenger_count"]

# 3. Calculate the number of trips by vendor
vendor_trip_count = df.groupBy("vendorId").agg(count("id").alias("trip_count"))

print(f"Average Trip Duration: {avg_trip_duration}")
print(f"Average Passenger Count: {avg_passenger_count}")
print("Number of trips by Vendor:")
vendor_trip_count.show()


analysis_results_path = f"s3://{args.processed_data_bucket}/{args.analysis_prefix}"
vendor_trip_count.write.mode("overwrite").json(analysis_results_path)
