#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Python script to run benchmark on a query with a file path.
Usage:
    $ spark-submit pq_avg_income.py <file_path>
'''


# Import command line arguments and helper functions
import sys
import bench

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession

def median(lst):
    n = len(lst)
    s = sorted(lst)
    return (sum(s[n//2-1:n//2+1])/2.0, s[n//2])[n % 2] if n else None

def pq_avg_income(spark, file_path):
    '''Construct a basic query on the people dataset

    This function returns a uncomputed dataframe that
    will compute the average income grouped by zipcode

    Parameters
    ----------
    spark : spark session object

    file_path : string
        The path (in HDFS) to the Parquet-backed file, e.g.,
        `hdfs:/user/{YOUR NETID}/people_small.parquet

    Returns
    df_avg_income:
        Uncomputed dataframe of average income grouped by zipcode
    '''

    #TODO
    f = spark.read.parquet(file_path)
    f.createOrReplaceTempView('f')

    res = spark.sql('SELECT f.zipcode, AVG(f.income) FROM f GROUP BY f.zipcode')
    return res



def main(spark, file_path):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    which_dataset : string, size of dataset to be analyzed
    '''
    #TODO
    times = bench.benchmark(spark, 5, pq_avg_income, file_path)

    print('Maximum Time taken to run average income 25 times on {}:{}'.format(file_path, max(times)))
    print('Minimum Time taken to run average income 25 times on {}:{}'.format(file_path, min(times)))

    # You can do list calculations for your analysis here!
    print('Average income median={:4f}'.format(median(times)))

# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    # Get file_path for dataset to analyze
    file_path = sys.argv[1]

    main(spark, file_path)
