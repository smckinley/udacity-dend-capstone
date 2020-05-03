import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession

def start_spark_session():
	spark = SparkSession.builder\
		.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
		.enableHiveSupport().getOrCreate()

	return spark

def extract_flight_data(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	for f in os.listdir(input_location):
		df = spark.read.format('com.github.saurfang.sas.spark')\
			.option('header', 'true')\
			.load(input_location + f)

		df.write.mode('append').parquet(output_location)

		# While testing just load the first file.
		break

def extract_airport_data(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.csv(input_location)

	df.write.mode('append').parquet(output_location)

def transform_flight_data(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.parquet(input_location)	

	
	
def transform_airport_data(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

def load_flight_dims():
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

def load_airport_dims():
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

def load_flight_facts():
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)



def main():
	print("Starting ETL")

	# Start Spark session.
	spark = start_spark_session()

	# Process ETL configurations.
	etl_config = configparser.ConfigParser()
	etl_config.read('etl.cfg')

	flight_data_location = etl_config['INPUT']['DATA_LOCATION_I94']

	airport_data_location = etl_config['INPUT']['DATA_LOCATION_AIRPORTS']

	extract_location = etl_config['OUTPUT']['DATA_LOCATION_EXTRACT']
	transform_location = etl_config['OUTPUT']['DATA_LOCATION_TRANSFORM']
	complete_location = etl_config['OUTPUT']['DATA_LOCATION_COMPLETE']

	# Read in flight information.
	flight_data_extract_dir = extract_location + "/sas_data/"
	extract_flight_data(
		flight_data_location, 
		flight_data_extract_dir, 
		spark)

	# Read in airport information.
	airport_data_extract_dir = extract_location + "/aiport_data/"
	extract_airport_data(
		airport_data_location,
		airport_data_extract_dir,
		spark)

	# Transform flight data.
	flight_data_transform_dir = transform_location + "/sas_data/"
	transform_flight_data(
		flight_data_extract_dir,
		flight_data_transform_dir,
		spark)

	# Transform airport data.
	airport_data_transform_dir = transform_location + "/airport_data/"
	transform_airport_data(
		airport_data_extract_dir,
		airport_data_transform_dir,
		spark)	

	# Load flight dimensions.
	#load_flight_dims()

	# Load airport dimensions.
	#load_airport_dims()

	# Load flight facts.
	#load_flight_facts()


if __name__ == "__main__":
    main()