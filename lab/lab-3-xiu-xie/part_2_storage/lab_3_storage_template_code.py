#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Template script to connect to Active Spark Session
Usage:
    $ spark-submit lab_3_storage_template_code.py <any arguments you wish to add>
    yarn logs -applicationId <your_application_id> -log_files stdout

    spark-submit --conf  spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=false --conf spark.dynamicAllocation.shuffleTracking.enabled=true
    spark-submit --py-files bench.py basic_query.py <your_data_file_path>
    hdfs:/user/bm106/pub/people_small.csv
    hdfs:/user/bm106/pub/people_medium.csv
    hdfs:/user/bm106/pub/people_large.csv
'''


# Import command line arguments and helper functions(if necessary)
import sys

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''
    #####--------------YOUR CODE STARTS HERE--------------#####

    #Use this template to as much as you want for your parquet saving and optimizations!

    ##### translate dataframes into parquet #####
    
    # read small
    # people_small = spark.read.csv('hdfs:/user/bm106/pub/people_small.csv', header=True, 
    #                         schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    # people_small.write.parquet('hdfs:/user/xx2179/people_small.parquet')

    # read medium
    # people_medium = spark.read.csv('hdfs:/user/bm106/pub/people_medium.csv', header=True, 
    #                         schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    # people_medium.write.parquet('hdfs:/user/xx2179/people_medium.parquet')

    # read large
    #people_large = spark.read.csv('hdfs:/user/bm106/pub/people_large.csv', header=True, 
    #                       schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    #people_large.write.parquet('hdfs:/user/xx2179/people_large.parquet')
    
    people = spark.read.csv('hdfs:/user/bm106/pub/people_small.csv', header=True,
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    people.createOrReplaceTempView('people')

    ##### OPTIMIZATION 1: SORT #####
    #people_sorted = people.orderBy('zipcode', 'income')
    #people_sorted.write.parquet('hdfs:/user/xx2179/people_sorted.parquet')


    ##### OPTIMIZATION 2: REPARTITION #####
    #people_repartition = people.repartition(10, 'zipcode')
    #people_repartition.write.parquet('hdfs:/user/xx2179/people_repartition.parquet')


    ##### OPTIMIZATION 3: SORT 2 #####
    people_sorted2 = people.orderBy('income', ascending = False)
    people_sorted2.write.parquet('hdfs:/user/xx2179/people_sorted2.parquet')


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
