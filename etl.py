import pandas as pd
import configparser
from os import listdir
from pyspark.sql import SparkSession

def extract_flight_data(input_location, output_location):
	print("Reading from " + input_location)
	print("Writing to " + output_location)

	spark = SparkSession.builder\
		.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
		.enableHiveSupport().getOrCreate()

	for f in listdir(input_location):
		df_spark = spark.read.format('com.github.saurfang.sas.spark')\
			.option('header', 'true')\
			.load(input_location + "/" + f)

		df_spark.write.mode('append').parquet(output_location + "sas_data")

def extract_airport_data(input_location, output_location):
	print("Reading from " + input_location)
	print("Writing to " + output_location)

def process_flight_dims():
	return 1;

def process_airport_dims():
	return 1;

def process_flight_facts():
	return 1;

def main():
	print("Starting ETL")

	# Process ETL configurations.
	etl_config = configparser.ConfigParser()
	etl_config.read('etl.cfg')

	flight_data_location = etl_config['INPUT']['DATA_LOCATION_I94']

	airport_data_location = etl_config['INPUT']['DATA_LOCATION_AIRPORTS']

	output_data_location = etl_config['OUTPUT']['DATA_LOCATION_OUTPUT']

	# Read in flight information.
	extract_flight_data(flight_data_location, output_data_location)

	# Read in airport information.
	extract_airport_data(airport_data_location, output_data_location)

	# Process flight dimensions.
	process_flight_dims()

	# Process airport dimensions.
	process_airport_dims()

	# Process flight facts.
	process_flight_facts()


if __name__ == "__main__":
    main()