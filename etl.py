import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession

def extract_flight_data(input_location, output_location):
	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + "/" + f)

	spark = SparkSession.builder\
		.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
		.enableHiveSupport().getOrCreate()

	for f in os.listdir(input_location):
		df_spark = spark.read.format('com.github.saurfang.sas.spark')\
			.option('header', 'true')\
			.load(input_location + "/" + f)

		df_spark.write.mode('append').parquet(output_location)

		# While testing just load the first file.
		break

def extract_airport_data(input_location, output_location):
	print("Reading from " + input_location)
	print("Writing to " + output_location)

def transform_flight_data():
	return 1

def transform_airport_data():
	return 1

def load_flight_dims():
	return 1

def load_airport_dims():
	return 1

def load_flight_facts():
	return 1

def main():
	print("Starting ETL")

	# Process ETL configurations.
	etl_config = configparser.ConfigParser()
	etl_config.read('etl.cfg')

	flight_data_location = etl_config['INPUT']['DATA_LOCATION_I94']

	airport_data_location = etl_config['INPUT']['DATA_LOCATION_AIRPORTS']

	extract_location = etl_config['OUTPUT']['DATA_LOCATION_EXTRACT']
	transform_location = etl_config['OUTPUT']['DATA_LOCATION_TRANSFORM']
	complete_location = etl_config['OUTPUT']['DATA_LOCATION_COMPLETE']

	# Read in flight information.
	flight_data_extract_dir = "sas_data"
	extract_flight_data(flight_data_location, extract_location + "/" \
		+ flight_data_extract_dir)

	# Read in airport information.
	extract_airport_data(airport_data_location, extract_location)

	# Transform flight data.
	transform_flight_data()

	# Transform airport data.
	transform_airport_data()	

	# Load flight dimensions.
	load_flight_dims()

	# Load airport dimensions.
	load_airport_dims()

	# Load flight facts.
	load_flight_facts()


if __name__ == "__main__":
    main()