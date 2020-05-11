# udacity-dend-capstone
Udacity Data Engineer Capstone Project

# Purpose
The process implemented in this project takes raw flight and airport data and
produces a data model suitable for analyzing flight patterns by gender across
states in the US. 

Additionally, data checks are implemented to ensure that
1. There are no duplicate airports in the airport dimension table.
2. There are no flights arriving in airports which are not listed in the
	airport table.

# Design
The general principle used in this ETL is to break the process up into 
discrete, independent steps. This allows for easier troubleshooting should
issues arise during processing and also allows makes it easier for the process
to be implemented using an orchestration platform like Airflow. At each step
in the process a set of files are produced and then the following step looks
for those files in an expected location.

Data locations are definable in the etl.cfg file. This allows the process to
be modified without updating the logic of the ETL.

A simple Spark-based ETL was used to keep the implementation simple yet
scalable. As flight information grows or processing requirements increase,
the process can be distributed to additional workers to handle the increased
load.

# Data Dictionary
Flight Dimensions
| Field | Definition |
|-------|------------|
| ID | Flight ID |
| YEAR | Year flight occurred |
| MONTH | Integer month of year flight occurred |
| AIRPORT_CODE | Alpha code identifying airport |
| CITY_ABBRV | Abbreviated city name where airport is located |
| BIRTH_YEAR | Birth year of traveler on travel day |
| GENDER | Gender (M/F) of traveler on travel day |

Airport Dimensions
| Field | Definition |
|-|-|
| AIRPORT_CODE | Alpha code identifying airport |
| COUNTRY | Country code of airport location |
| REGION | Region airport is located in |
| LATITUDE | Latitude of airport |
| LONGITUDE | Longitude of airport |

Flight Facts 
| Field | Definition |
|-|-|
| ID | Flight ID |
| AIRPORT_CODE | Airport code |
| MALE_COUNT | A 1 or 0 indicating if the traveler was a male |
| FEMALE_COUNT | A 1 or 0 indicating if the traveler was female |



# Additional Considerations
In the case that the volume of data increases by a factor of 100 or more, the
Spark-based implementation allows it to be scaled up by purchasing additional
computing resources and distributing the job.

If a Service Level Agreement required that data be made available at a
particular time of day, an orchestration tool like Airflow would allow us to
set a time deadline as a requirement and report an error if this deadline was
not met.

If the data needed to be made available to large number of users, scheduling
the process and distributed access to the completed, transformed files would
provide such access. Alternatively, a cloud-based BI tool could be used to 
provide access the data.