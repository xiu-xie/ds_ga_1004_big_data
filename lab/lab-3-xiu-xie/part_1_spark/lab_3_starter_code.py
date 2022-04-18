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
import pyspark.sql.functions as fn
#from pyspark.sql.functions import percentile

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
    reserves = spark.read.json(f'hdfs:/user/{netID}/reserves.json')
    artist_term = spark.read.csv(f'hdfs:/user/{netID}/artist_term.csv', schema = 'artistID STRING, term STRING')
    tracks = spark.read.csv(f'hdfs:/user/{netID}/tracks.csv', schema = 'trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')

    print('Printing boats inferred schema')
    boats.printSchema()
    print('Printing sailors inferred schema')
    sailors.printSchema()
    print('Printing reserves inferred schema')
    reserves.printSchema()
    print('Printing artist_term inferred schema')
    artist_term.printSchema()
    print('Printing tracks inferred schema')
    tracks.printSchema()
    # Why does sailors already have a specified schema?

    print('Reading boats.txt and specifying schema')
    boats = spark.read.csv('boats.txt', schema='bid INT, bname STRING, color STRING')

    print('Printing boats with specified schema')
    boats.printSchema()

    # Give the dataframe a temporary view so we can run SQL queries
    boats.createOrReplaceTempView('boats')
    sailors.createOrReplaceTempView('sailors')
    reserves.createOrReplaceTempView('reserves')
    artist_term.createOrReplaceTempView('artist_term')
    tracks.createOrReplaceTempView('tracks')
                            
    # Construct a query
    print('Example 1: Executing SELECT count(*) FROM boats with SparkSQL')
    query = spark.sql('SELECT count(*) FROM boats')

    # Print the results to the console
    query.show()

    #####--------------YOUR CODE STARTS HERE--------------#####

    #make sure to load reserves.json, artist_term.csv, and tracks.csv
    #For the CSVs, make sure to specify a schema!
    
    ###############
    ### Question 1###
    ###############
    
    print('Question 1')
    question_1_query = spark.sql('''SELECT sid, sname, age FROM sailors WHERE age > 40''')
    question_1_query.show()

    ###############
    ### Question 2###
    ###############

    print('Question 2')
    #spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')
    question_2_oi = reserves.filter(reserves.bid != 101).groupBy('sid').agg(fn.count('bid').alias('num_boats'))
    question_2_oi.show()

    
    ###############
    ### Question 3###
    ###############

    print('Question 3')
    question_3_query = spark.sql('''SELECT FIRST(sailors.sid) as sid,
            sailors.sname,
            COUNT(DISTINCT reserves.bid) as num_boats
        FROM sailors
        LEFT JOIN reserves
            ON sailors.sid = reserves.sid
        GROUP BY sailors.sid, sailors.sname''')
    question_3_query.show()


    ###############
    ### Question 4###
    ###############

    print('Question 4')
    ## left join??
    question_4_query = spark.sql('''
        select percentile(tracks.year, 0.5) as median_year,
           max(tracks.duration) as max_duration,
           count(distinct artist_term.artistID) as num_artists,
           artist_term.term
        from artist_term
        join tracks
           on artist_term.artistID = tracks.artistID
        group by term
        order by avg(tracks.duration) asc
        limit 10
        ''')
    question_4_query.show()

    

    ###############
    ### Question 5###
    ###############

    #number of distinct tracks associated (through artistID) to each term
    #return only the top 10 most popular terms, and again for the bottom 10
    print('Question 5')
    question_5a_query = spark.sql('''select count(distinct trackID) as count_tracks, term
        from artist_term
        left join tracks
            on artist_term.artistID = tracks.artistID
        group by term
        order by count_tracks desc
        limit 10''')
    question_5a_query.show()

    question_5b_query = spark.sql('select count(distinct trackID) as count_tracks, term from artist_term left join tracks on artist_term.artistID = tracks.artistID group by term order by count_tracks asc limit 10')
    question_5b_query.show()
    
# spark-submit --conf  spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=false --conf spark.dynamicAllocation.shuffleTracking.enabled=true lab_3_starter_code.py
# yarn logs -applicationId <your_application_id> -log_files stdout
# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part1').getOrCreate()

    # Get user netID from the command line
    netID = getpass.getuser()

    # Call our main routine
    main(spark, netID)
