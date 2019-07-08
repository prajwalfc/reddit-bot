# Data Engineering

## Bot detection on reddit using user activity and the comments they make

## Team

For now I am only the one.

 
### Getting started team
Read, learn, memorize and *practice* the following:
 - python
   - https://docs.python.org/3/tutorial/
   - https://docs.python.org/3/library/
   - https://docs.python.org/3/reference/datamodel.html
 
 
 - spark:  
 from http://spark.apache.org/docs/latest/sql-programming-guide.html
   - http://spark.apache.org/docs/latest/sql-data-sources-avro.html
   - http://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
   - http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
   - http://spark.apache.org/docs/latest/sql-data-sources-json.html
   - http://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
   - http://spark.apache.org/docs/latest/sql-data-sources-orc.html
   - http://spark.apache.org/docs/latest/sql-data-sources-parquet.html
   - http://spark.apache.org/docs/latest/sql-data-sources-troubleshooting.html
   - http://spark.apache.org/docs/latest/sql-data-sources.html
   - http://spark.apache.org/docs/latest/sql-distributed-sql-engine.html
   - http://spark.apache.org/docs/latest/sql-getting-started.html
   - http://spark.apache.org/docs/latest/sql-migration-guide-hive-compatibility.html
   - http://spark.apache.org/docs/latest/sql-migration-guide-upgrade.html
   - http://spark.apache.org/docs/latest/sql-migration-guide.html
   - http://spark.apache.org/docs/latest/sql-performance-tuning.html
   - http://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html
   - http://spark.apache.org/docs/latest/sql-reference.html

from pyspark python documentation:
   - http://spark.apache.org/docs/latest/api/python/pyspark.html
   - http://spark.apache.org/docs/latest/api/python/pyspark.sql.html

## Principles

 - Python is the default language for the data stack.
 - Engineers over Scientists
 - Convention over Configuration over Coding.
 - More thinking less typing
 - Keep it simple
 - Re-use over Integrate over Build.
 - Service Oriented (CLI, Web HTTP APIs , Python Libraries)
 - Be kind, be curious

### Tools Specific Principles
#### Spark
Spark is a library with many cohexisting layers, mostly because of back compatibility some of this APIs are still around both in the tool as well on the web with many Q&A still going round. When learning Spark please follow the following principles:

 - pyspark only (https://spark.apache.org/docs/latest/api/python/)
 - Learn ONLY the modules: pyspark.sql and pyspark.ml
 - Skip anything related to Scala, Java, R (according to the above general principle)
 - Skip categorically anything about RDDs, Map-Reduce, and MLlib
 
 
## Hardware

-Currently

1 Master.
	-	6 cores, 6GB Memory, 100 EBS
4 Workers
	-	8 cores, 32GB Memory, 100 EBS


## Use cases

 -  Visulaize bot activites 
 -  Track and block bad bots
 -  Chatbots

## Stretch Goals
 
 -	The project is solely based on detecting bots with metrics that derives from timestamp and comments(text).
 -	So not all bots could be detetced if they throw in subtle sign and almost close to pass Turing Test.
 -	This project will be part of Machine learning pipeline where the obtained datasets could be part of Training data sets.