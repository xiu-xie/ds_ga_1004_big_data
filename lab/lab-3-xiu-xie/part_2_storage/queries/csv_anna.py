#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit csv_anna.py <file_path>

    spark-submit --py-files bench.py csv_anna.py <your_data_file_path>
    yarn logs -applicationId <your_application_id> -log_files stdout
    
    hdfs:/user/bm106/pub/people_small.csv
    hdfs:/user/bm106/pub/people_medium.csv
    hdfs:/user/bm106/pub/people_large.csv
'''


# Import command line arguments and helper functions
import sys
import bench
import statistics

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession


def csv_anna(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will filters down to only include people with `first_name`
    of 'Anna' and income at least 70000

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the CSV file, e.g.,
        `hdfs:/user/bm106/pub/people_small.csv`

    Returns
    df_anna:
        Uncomputed dataframe that only has people with 
        first_name of 'Anna' and income at least 70000
    '''

    #TODO
    people = spark.read.csv(file_path, header=True, 
                            schema='first_name STRING, last_name STRING, income FLOAT, zipcode INT')
    
    people.createOrReplaceTempView('people')

    df_anna = spark.sql('''SELECT * FROM people WHERE first_name = 'Anna' and income >= 70000''')

    return df_anna




def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    #TODO
    times = bench.benchmark(spark, 25, csv_anna, file_path)

    print(f'Maximum Time taken to run Basic Query 25 times on {file_path}:{max(times)}')
    print(f'Median Time taken to run Basic Query 25 times on {file_path}:{statistics.median(times)}')
    print(f'Minimum Time taken to run Basic Query 25 times on {file_path}:{min(times)}')


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
