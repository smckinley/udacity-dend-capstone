# ETL process for digesting and reporting on flight information.

import pandas as pd
import configparser
import os
from pyspark.sql import SparkSession

def start_spark_session():
	"""Start a Spark session and return the object for use."""
	spark = SparkSession.builder\
		.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
		.enableHiveSupport().getOrCreate()

	return spark

def extract_flight_data(input_location, output_location, spark_session):
	"""Read flight data from the file location provided and load it into a 
	staging table.

    Keyword arguments:
    input_location -- the location of the raw flight data
    output_location -- directory location for storing the extracted data
    spark_session -- a Spark session for processing the data
	"""
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
	"""Read airport from the file location provided and load it into a 
	staging table.

    Keyword arguments:
    input_location -- the location of the raw airport data
    output_location -- directory location for storing the extracted data
    spark_session -- a Spark session for processing the data
	"""	
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.format('csv').options(header='true').load(input_location)

	df.write.mode('overwrite').parquet(output_location)

def transform_flight_data(input_location, output_location, spark_session):
	"""Transform flight data into analytical facts and dimensions.

    Keyword arguments:
    input_location -- the location of the extracted flight data
    output_location -- directory location for storing the transformed data
    spark_session -- a Spark session for processing the data
	"""	
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.parquet(input_location)	

	df.createOrReplaceTempView('flight_data_extract_vw')

	df_transformed = spark.sql("""
		select
			cast(cicid as int) as id,
			cast(i94yr as int) as year,
			cast(i94mon as int) as month,
			cast(i94port as string) as airport_code,
			cast(i94addr as string) as city_abbrv,
			cast(biryear as int) as birth_year,
			cast(gender as string) as gender,
			case when gender = 'M' then 1 else 0 end male_count,
			case when gender = 'F' then 1 else 0 end female_count
		from flight_data_extract_vw
		""")

	df_transformed.write.mode('overwrite').parquet(output_location)

def transform_airport_data(input_location, output_location, spark_session):
	"""Transform airport data into analytical facts and dimensions.

    Keyword arguments:
    input_location -- the location of the extracted airport data
    output_location -- directory location for storing the transformed data
    spark_	
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.parquet(input_location)

	df.createOrReplaceTempView('airport_data_extract_vw')

	df_transformed = spark.sql("""
		select distinct
			cast(iata_code as string) as airport_code,
			cast(iso_country as string) as country,
			cast(iso_region as string) as region,
			cast(substr(coordinates, 1, instr(coordinates, ',') - 1) as double) as latitude,
			cast(substr(coordinates, instr(coordinates, ',') + 1, length(coordinates)) as double) as longitude
		from airport_data_extract_vw
		where iata_code is not null
		""")

	df_transformed.write.mode('overwrite').parquet(output_location)

def load_flight_dims(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.parquet(input_location)

	df.createOrReplaceTempView('flight_data_transform_vw')

	df_load = spark.sql("""
		select
			id,
			year,
			month,
			airport_code,
			city_abbrv,
			birth_year,
			gender
		from flight_data_transform_vw
		""")

	df_load.write.mode('overwrite').parquet(output_location)


def load_airport_dims(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.parquet(input_location)

	df.createOrReplaceTempView('airport_data_transform_vw')

	df_load = spark.sql("""
		select
			airport_code,
			country,
			region,
			latitude,
			longitude
		from airport_data_transform_vw
		""")

	df_load.write.mode('overwrite').parquet(output_location)

def load_flight_facts(input_location, output_location, spark_session):
	spark = spark_session

	if os.path.exists(output_location):
		for f in os.listdir(output_location):
			os.remove(output_location + f)

	df = spark.read.parquet(input_location)

	df.createOrReplaceTempView('flight_data_transform_vw')

	df_load = spark.sql("""
		select
			id,
			airport_code,
			male_count,
			female_count
		from flight_data_transform_vw
		""")

	df_load.write.mode('overwrite').parquet(output_location)

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
	flight_data_extract_dir = extract_location + "sas_data/"
	# extract_flight_data(
	# 	flight_data_location, 
	# 	flight_data_extract_dir, 
	# 	spark)

	# Read in airport information.
	airport_data_extract_dir = extract_location + "airport_data/"
	extract_airport_data(
		airport_data_location,
		airport_data_extract_dir,
		spark)

	# Transform flight data.
	flight_data_transform_dir = transform_location + "sas_data/"
	transform_flight_data(
		flight_data_extract_dir,
		flight_data_transform_dir,
		spark)

	# Transform airport data.
	airport_data_transform_dir = transform_location + "airport_data/"
	transform_airport_data(
		airport_data_extract_dir,
		airport_data_transform_dir,
		spark)	

	# Load flight dimensions.
	flight_dims_load_dir = complete_location + "flight_dims/"
	load_flight_dims(
		flight_data_transform_dir,
		flight_dims_load_dir,
		spark)

	# Load airport dimensions.
	airport_dims_load_dir = complete_location + "airport_dims/"
	load_airport_dims(
		airport_data_transform_dir,
		airport_dims_load_dir,
		spark)

	# Load flight facts.
	flight_facts_load_dir = complete_location + "flight_facts/"
	load_flight_facts(
		flight_data_transform_dir,
		flight_facts_load_dir,
		spark)

	# Check 1: No duplicate airports
	df = spark.read.parquet(airport_dims_load_dir)
	df.createOrReplaceTempView('v')

	df_test = spark.sql("""
		select airport_code, count(*) duplicate_count
		from v
		group by airport_code
		having count(*) > 1
		""")

	if df_test.count() > 0:
		print("ERROR: Duplicate entries found for airport codes.")
		df_test.show()

	# Check 2: Flights arrived at unidentified airports
	df_airports = spark.read.parquet(airport_dims_load_dir)
	df_flights  = spark.read.parquet(flight_dims_load_dir)

	df_airports.createOrReplaceTempView('v')
	df_flights.createOrReplaceTempView('f')

	df_test = spark.sql("""
		select f.airport_code, count(*) n
		from f
		where not exists (
			select 1
			from v 
			where v.airport_code = f.airport_code
		)
		group by f.airport_code
		""")

	if df_test.count() > 0:
		print("ERROR: Flights arriving at unidentified airports.")
		df_test.show()	

	print("ETL complete.")


if __name__ == "__main__":
    main()