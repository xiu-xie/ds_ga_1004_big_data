#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''Starter Pyspark Script for students to complete for their Lab 3 Assignment.
Usage:
    $ spark-submit lab_3_starter_code.py <student_netID>
'''
#Use getpass to obtain user netID
import getpass

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as fn


def main(spark, netID):
    '''Main routine for Lab Solutions
    Parameters
    ----------
    spark : SparkSession object
    netID : string, netID of student to find files in HDFS
    '''
    print('Lab 3 Example dataframe loading and SQL query')

    # Load the boats.txt and sailors.json data into DataFrame
    boats = spark.read.csv(f'hdfs:/user/{netID}/boats.txt')
    sailors = spark.read.json(f'hdfs:/user/{netID}/sailors.json')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    #Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!

    #Q1
    # question_1_query = spark.sql('SELECT sid, sname, age FROM sailors WHERE rating > 6')
    # question_1_query.show()

    # Q2
    # reserves = spark.read.json(f'hdfs:/user/{netID}/reserves.json')
    # reserves.createOrReplaceTempView('reserves')
    # question_2_query = reserves.filter(reserves.bid != 101).groupBy('sid').agg(fn.count('bid').alias('count'))
    # question_2_query.show()

    # Q3
    # question_3_query = spark.sql('SELECT sailors.sid, sailors.sname, COUNT(DISTINCT(reserves.bid)) FROM sailors INNER JOIN reserves ON sailors.sid = reserves.sid GROUP BY sailors.sid, sailors.sname')
    # question_3_query.show()

    artist_term = spark.read.csv('artist_term.csv', schema='artistID STRING, term STRING')
    tracks = spark.read.csv('tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')
    artist_term.createOrReplaceTempView('artist_term')
    tracks.createOrReplaceTempView('tracks')

    # Q4
    # question_4_query = spark.sql('SELECT artist_term.term, MIN(year), AVG(duration), COUNT(DISTINCT(tracks.artistID)) 
    # FROM tracks LEFT JOIN artist_term ON tracks.artistID = artist_term.artistID GROUP BY artist_term.term ORDER BY AVG(tracks.duration) DESC LIMIT 10')
    
    question_4_query = tracks.join(artist_term,['artistID'],'left').groupby(artist_term.term).agg(fn.min(tracks.year),fn.avg(tracks.duration).alias('avg'),fn.count(tracks.artistID)).orderBy(fn.col('avg').desc())
    question_4_query.show(10)

    # Q5

    question_5_query = tracks.join(artist_term,['artistID']).groupBy(artist_term.term).agg(fn.countDistinct(tracks.trackID))
    question_5_query.show()
    question_5_query_top_10 = tracks.join(artist_term,['artistID']).groupBy(artist_term.term).agg(fn.countDistinct(tracks.trackID).alias('countDistinct')).orderBy(fn.col('countDistinct').desc())
    question_5_query_top_10.show(10)
    question_5_query_bottom_10 = tracks.join(artist_term,['artistID']).groupBy(artist_term.term).agg(fn.countDistinct(tracks.trackID).alias('countDistinct')).orderBy(fn.col('countDistinct').asc())
    question_5_query_bottom_10.show(10)


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, netID)
