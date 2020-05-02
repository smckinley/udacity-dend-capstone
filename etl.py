import pandas as pd
import configparser
from pyspark.sql import SparkSession

def extract_flight_data():
	return 1;

def extract_airport_data():
	return 1;

def process_flight_dims():
	return 1;

def process_airport_dis():
	return 1;

def process_flight_facts():
	return 1;

def main():
	print("Starting ETL")

	# Read in flight information.
	extract_flight_data()

	# Read in airport information.
	extract_airport_data()

	# Process flight dimensions.
	process_flight_dims()

	# Process airport dimensions.
	process_airport_dims()

	# Process flight facts.
	process_flight_facts()


if __name__ == "__main__":
    main()