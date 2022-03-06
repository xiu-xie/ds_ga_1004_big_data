# Lab 3: Spark and Parquet Optimization Report

Name:
 
NetID: 

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.rating > 6).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

SELECT sid, sname, age FROM sailors WHERE rating > 6

```

Output:
```
+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 32|   andy|25.5|
| 58|  rusty|35.0|
| 64|horatio|16.0|
| 71|  zorba|35.0|
| 74|horatio|25.5|
+---+-------+----+
```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python
import pyspark.sql.functions as fn

reserves = spark.read.json(f'hdfs:/user/{netID}/reserves.json')
reserves.createOrReplaceTempView('reserves')
question_2_query = reserves.filter(reserves.bid != 101).groupBy('sid').agg(fn.count('bid').alias('count'))
question_2_query.show()

```


Output:
```
+---+-----+
|sid|count|
+---+-----+
| 22|    3|
| 31|    3|
| 74|    1|
| 64|    1|
+---+-----+
```

#### Question 3: 
Using SQL and (multiple) inner joins, in a single query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL
question_3_query = spark.sql('SELECT sailors.sid, sailors.sname, COUNT(DISTINCT(reserves.bid)) FROM sailors INNER JOIN reserves ON sailors.sid = reserves.sid GROUP BY sailors.sid, sailors.sname')
question_3_query.show()
```


Output:
```
+---+-------+-------------------+
|sid|  sname|count(DISTINCT bid)|
+---+-------+-------------------+
| 64|horatio|                  2|
| 22|dusting|                  4|
| 31| lubber|                  3|
| 74|horatio|                  1|
+---+-------+-------------------+
```

#### Question 4: 
Repeating the analysis from Q6.2 in Lab2, implement a query using Spark transformations
which finds for each artist term, compute the minimum year of release,
average track duration, and the total number of artists for that term (by ID).
What are the results for the ten terms with the longest average track durations?  
Include both your query code and resulting DataFrame in your response.

Code:
```python
artist_term = spark.read.csv('artist_term.csv', schema='artistID STRING, term STRING')
tracks = spark.read.csv('tracks.csv', schema='trackID STRING, title STRING, release STRING, year INT, duration FLOAT, artistID STRING')
artist_term.createOrReplaceTempView('artist_term')
tracks.createOrReplaceTempView('tracks')

question_4_query = tracks.join(artist_term,['artistID'],'left').groupby(artist_term.term).agg(fn.min(tracks.year),fn.avg(tracks.duration).alias('avg'),fn.count(tracks.artistID)).orderBy(fn.col('avg').desc())
question_4_query.show(10)
```


Output:
```
+--------------------+---------+------------------+------------------------+
|                term|min(year)|     avg(duration)|count(DISTINCT artistID)|
+--------------------+---------+------------------+------------------------+
|              bhajan|        0|1147.2648391723633|                       2|
|         svart metal|     2010| 1003.232177734375|                       1|
|  experimental noise|        0|1001.5255482991537|                       2|
|          mixed jazz|     1998|  950.281982421875|                       1|
|   heavy psychedelic|     1996| 917.0281372070312|                       1|
|  middle eastern pop|        0| 845.2958374023438|                       1|
|           pakistani|        0| 810.4124545142764|                       3|
|              nippon|        0| 805.9815979003906|                       2|
|electronic music ...|        0| 768.0648345947266|                       1|
|    early electronic|        0| 768.0648345947266|                       1|
+--------------------+---------+------------------+------------------------+
```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated
(through artistID) to each term.  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

##### Initial query
Code:
```python
question_5_query = tracks.join(artist_term,['artistID']).groupBy(artist_term.term).agg(fn.countDistinct(tracks.trackID))
question_5_query.show()
```
Output:
```
+--------------------+-----------------------+
|                term|count(DISTINCT trackID)|
+--------------------+-----------------------+
|   singer-songwriter|                   5593|
|             melodic|                   1387|
|  adult contemporary|                    673|
| gramusels bluesrock|                     58|
|               anime|                    234|
|        haldern 2008|                     10|
|              poetry|                    403|
|             lyrical|                    535|
|   traditional metal|                     36|
|electronica latin...|                      4|
|            priority|                      8|
|   polish electronic|                      2|
|        german metal|                     77|
|          indigenous|                      8|
|      french electro|                     16|
|     swedish hip hop|                      9|
|         indie music|                     16|
|        pukkelpop 07|                      1|
|           scenecore|                      1|
|            dub rock|                      8|
+--------------------+-----------------------+
```

##### 10 Most Popular Terms
Code:
```python
question_5_query_top_10 = tracks.join(artist_term,['artistID']).groupBy(artist_term.term).agg(fn.countDistinct(tracks.trackID).alias('countDistinct')).orderBy(fn.col('countDistinct').desc())
question_5_query_top_10.show(10)
```
Output:
```python
+----------------+-------------+
|            term|countDistinct|
+----------------+-------------+
|            rock|        21796|
|      electronic|        17740|
|             pop|        17129|
|alternative rock|        11402|
|         hip hop|        10926|
|            jazz|        10714|
|   united states|        10345|
|        pop rock|         9236|
|     alternative|         9209|
|           indie|         8569|
+----------------+-------------+
```

##### 10 Least Popular Terms
Code:
```python
question_5_query_bottom_10 = tracks.join(artist_term,['artistID']).groupBy(artist_term.term).agg(fn.countDistinct(tracks.trackID).alias('countDistinct')).orderBy(fn.col('countDistinct').asc())
question_5_query_bottom_10.show(10)
```

Output:
```
+-----------------+-------------+
|             term|countDistinct|
+-----------------+-------------+
|       jazz organ|            1|
|     italian beat|            1|
|        scenecore|            1|
|      polish kicz|            1|
|     reading 2007|            1|
|  indie argentina|            1|
|            watmm|            1|
|broadway musicals|            1|
|     pukkelpop 07|            1|
|       micromusic|            1|
+-----------------+-------------+
```

## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  - What did you try in part 2.5 to improve performance for each query?
  - What worked, and what didn't work?
 
For Part 2.3 and 2.4:

|        | pq_anna     |           |             | pq_avg_income  |           |             | pq_max_income  |           |             |
|--------|-------------|-----------|-------------|----------------|-----------|-------------|----------------|-----------|-------------|
|        | min         | median    | max         | min            | median    | max         | min            | median    | max         |
| small  | 0.103130102 | 0.14578   | 2.364332676 | 0.554216146    | 0.642674  | 3.16598177  | 0.439177275    | 0.492058  | 3.479733706 |
| medium | 0.130729437 | 0.177948  | 3.660793543 | 0.914097309    | 1.154936  | 5.819596291 | 0.485636234    | 0.601003  | 158.9210813 |
| large  | 2.022039175 | 2.58344   | 61.45129228 | 4.720864296    | 4.823783  | 10.47385383 | 3.574396372    | 5.287887  | 134.4699988 |
|        |             |           |             |                |           |             |                |           |             |
|        | csv_anna    |           |             | csv_avg_income |           |             | csv_max_income |           |             |
|        | min         | median    | max         | min            | median    | max         | min            | median    | max         |
| small  | 0.10123086  | 0.138377  | 4.111708403 | 0.602245331    | 0.907135  | 4.718338013 | 0.536713839    | 0.746702  | 6.354320765 |
| medium | 0.421193123 | 0.456694  | 3.74266839  | 0.937530994    | 1.383407  | 4.87706852  | 1.008648396    | 1.365082  | 6.182369947 |
| large  | 26.67442179 | 27.115692 | 156.6734617 | 44.8383944     | 94.833008 | 271.3226349 | 27.65533018    | 29.191193 | 151.4906929 |

In general, query is faster with parquet, especially on large datasets.

For 2.5：

|                                   | Dataset | pq_max_income |           |             |
|-----------------------------------|---------|---------------|-----------|-------------|
|                                   |         | min           | median    | max         |
| Original                          | large   | 3.574396372   | 5.287887  | 134.4699988 |
| Sort by income                    | large   | 18.88579011   | 42.734242 | 580.7045143 |
| Sort by last name                 | large   | 6.722538471   | 19.6904   | 254.9252939 |
| Repartition into 50, by last name | large   | 0.446079493   | 0.576926  | 87.87013912 |
| replication factor=5              | large   | 5.66821456    | 6.089855  | 11.4512105  |

Observation: 
1. Sorting does not work. 
2. Best repartition factor is 50, repartition by last name.
3. Replication factor = 5 yields about the same median than benchmark, but has way faster max_time