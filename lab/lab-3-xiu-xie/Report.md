# Lab 3: Spark and Parquet Optimization Report

Name: Victoria Xie
 
NetID: xx2179

## Part 1: Spark

#### Question 1: 
How would you express the following computation using SQL instead of the object interface: `sailors.filter(sailors.age > 40).select(sailors.sid, sailors.sname, sailors.age)`?

Code:
```SQL

SELECT sid, sname, age FROM sailors WHERE age > 40

```


Output:
```

+---+-------+----+
|sid|  sname| age|
+---+-------+----+
| 22|dusting|45.0|
| 31| lubber|55.5|
| 95|    bob|63.5|
+---+-------+----+

```


#### Question 2: 
How would you express the following using the object interface instead of SQL: `spark.sql('SELECT sid, COUNT(bid) from reserves WHERE bid != 101 GROUP BY sid')`?

Code:
```python

reserves.filter(reserves.bid != 101).groupBy('sid').agg(fn.count('bid').alias('num_boats'))

```


Output:
```

+---+---------+
|sid|num_boats|
+---+---------+
| 22|        3|
| 31|        3|
| 74|        1|
| 64|        1|
+---+---------+


```

#### Question 3: 
Using a single SQL query, how many distinct boats did each sailor reserve? 
The resulting DataFrame should include the sailor's id, name, and the count of distinct boats. 
(Hint: you may need to use `first(...)` aggregation function on some columns.) 
Provide both your query and the resulting DataFrame in your response to this question.

Code:
```SQL

	SELECT FIRST(sailors.sid) as sid,
            sailors.sname,
            COUNT(DISTINCT reserves.bid) as num_boats
        FROM sailors
        LEFT JOIN reserves
            ON sailors.sid = reserves.sid
        GROUP BY sailors.sid, sailors.sname

```


Output:
```

+---+-------+---------+
|sid|  sname|num_boats|
+---+-------+---------+
| 58|  rusty|        0|
| 64|horatio|        2|
| 29| brutus|        0|
| 22|dusting|        4|
| 31| lubber|        3|
| 71|  zorba|        0|
| 85|    art|        0|
| 74|horatio|        1|
| 95|    bob|        0|
| 32|   andy|        0|
+---+-------+---------+


```

#### Question 4: 
Implement a query using Spark transformations which finds for each artist term, compute the median year of release, maximum track duration, and the total number of artists for that term (by ID).
  What are the results for the ten terms with the shortest *average* track durations?
  Include both your query code and resulting DataFrame in your response.


Code:
```python

	select percentile(tracks.year, 0.5) as median_year, term,
            max(tracks.duration) as max_duration,
           count(distinct artist_term.artistID) as num_artists
        from artist_term
        join tracks
           on artist_term.artistID = tracks.artistID
        group by term
        order by avg(tracks.duration) asc
        limit 10

```


Output:
```

+-----------+------------+-----------+----------------+
|median_year|max_duration|num_artists|            term|
+-----------+------------+-----------+----------------+
|        0.0|    13.66159|          1|       mope rock|
|        0.0|    15.46404|          1|      murder rap|
|     2000.0|    25.91302|          1|    abstract rap|
|     2000.0|    25.91302|          1|experimental rap|
|        0.0|    26.46159|          1|     ghetto rock|
|        0.0|    26.46159|          1|  brutal rapcore|
|        0.0|    41.29914|          1|     punk styles|
|     1993.0|   145.89342|          1|     turntablist|
|        0.0|    45.08689|          1| german hardcore|
|     2005.0|    89.80853|          2|     noise grind|
+-----------+------------+-----------+----------------+

```
#### Question 5: 
Create a query using Spark transformations that finds the number of distinct tracks associated (through artistID) to each term.
  Modify this query to return only the top 10 most popular terms, and again for the bottom 10.
  Include each query and the tables for the top 10 most popular terms and the 10 least popular terms in your response. 

Code:
```SQL
	select count(distinct trackID) as count_tracks, term
        from artist_term
        left join tracks
        		on artist_term.artistID = tracks.artistID
        group by term
        order by count_tracks desc
        limit 10
        
        select count(distinct trackID) as count_tracks, term 
        from artist_term 
        left join tracks 
        		on artist_term.artistID = tracks.artistID 
        group by term 
        order by count_tracks asc 
        limit 10')
  
```
Output:
```
+------------+----------------+
|count_tracks|            term|
+------------+----------------+
|       21796|            rock|
|       17740|      electronic|
|       17129|             pop|
|       11402|alternative rock|
|       10926|         hip hop|
|       10714|            jazz|
|       10345|   united states|
|        9236|        pop rock|
|        9209|     alternative|
|        8569|           indie|
+------------+----------------+

+------------+--------------------+
|count_tracks|                term|
+------------+--------------------+
|           0|           metalgaze|
|           0| belgian black metal|
|           0|russian drum and ...|
|           0|    stonersludgecore|
|           0|    shemale vocalist|
|           0|      trance melodic|
|           0|       galante music|
|           0|       simerock 2008|
|           0|           alvaroidm|
|           0| finnish death metal|
+------------+--------------------+
```

## Part 2: Parquet Optimization:

What to include in your report:
  - Tables of all numerical results (min, max, median) for each query/size/storage combination for part 2.3, 2.4 and 2.5.
  
  ### 2.3
  
  Query | Size | Max | Median | Min
 --- | --- | --- | --- | ---
  csv_avg_income | small | 4.575184345245361 | 0.7366690635681152 | 0.644087553024292
  csv_avg_income | medium | 65.86392831802368 | 1.3446431159973145 | 1.184417963027954
  csv_avg_income | large | 30.348625421524048 | 10.751522541046143 | 7.281318426132202
  csv_max_income | small | 4.502741813659668 | 0.8847708702087402 | 0.7422506809234619
  csv_max_income | medium | 8.720243453979492 | 1.2500226497650146 | 1.087057113647461
  csv_max_income | large | 18.241158723831177 | 7.2126784324646 | 6.576931715011597
  csv_anna | small | 2.8158249855041504 | 0.0989372730255127 | 0.07913064956665039
  csv_anna | medium | 3.8809494972229004 | 1.1784958839416504 | 0.6338977813720703
  csv_anna | large | 13.335976362228394 | 5.634191036224365 | 5.308367729187012
  
    
  ### 2.4
  
   Query | Size | Max | Median | Min
 --- | --- | --- | --- | ---
pq_avg_income | small | 3.957742691040039 | 0.9374730587005615 | 0.717667818069458 
pq_avg_income | medium | 3.7264060974121094 | 0.9269618988037109 | 0.7957730293273926
pq_avg_income | large | 11.132874488830566 | 3.6970949172973633 | 3.4880785942077637
pq_max_income | small | 3.31514835357666 | 0.7714879512786865 | 0.6363070011138916
pq_max_income | medium | 15.95443058013916 | 4.634495258331299 | 3.739468574523926
pq_max_income | large | 8.756789445877075 | 4.927220106124878 | 4.355566740036011
pq_anna | small | 1.651972770690918 | 0.1122734546661377 | 0.08335137367248535
pq_anna | medium |1.7626547813415527 | 0.14322161674499512 | 0.11395144462585449
pq_anna | large | 6.143319368362427 | 1.0066728591918945 | 1.0066728591918945


  ### 2.5
  
  #### sort
  
  Query | Max | Median | Min
   --- | --- | --- | --- 
   pq_anna | 3.9205076694488525 | 0.5667850971221924 | 0.49545860290527344
   pq_avg_income | 5.364859580993652 | 0.8156797885894775 | 0.5085346698760986
  pq_max_income | 10.755784273147583 | 5.2164716720581055 | 4.0932183265686035
  
  #### repartition
  
  Query | Max | Median | Min
   --- | --- | --- | --- 
   pq_anna | 36.85057353973389 | 10.103806734085083 | 3.2340004444122314
   pq_avg_income | 58.97018003463745 | 4.140972852706909 | 3.6680591106414795
   

  - How do the results in parts 2.3, 2.4, and 2.5 compare?
  
  Runtime has generally decreased after the datasets have been converted from csv to Parquet format, especially for `people_large`. The optimizing techniques I experimented with did not enhance the runtime performance of the 3 datasets. In fact, I experienced a steep increase in runtime with both sorting and repartition methods.
  
  - What did you try in part 2.5 to improve performance for each query?
  
  Initially, I wanted to try out 2 sorting and 1 repartition methods on all 3 datasets of different sizes. However, due to technical and time constraints (I constantly ran into runtime errors, even with the fix that Professor McFee posted), I was only able to conduct two of the optimizing techniques on the small dataset. 
  
  I first sorted the DataFrame based on both `zipcode` and `income` columns, since those take relatively small numbers of unique values and thus sorting may speed up the lookup time. Then I tried repartitioning the DataFrame by 10 partitions based on the `zipcode` column. I wrote both modified DataFrames into Parquet format and passed them to the 3 scripts.
  
  - What worked, and what didn't work?
  
  Neither of the techniques sped up the runtime in general, although in hindsight I believe if I modified the DataFrames in different ways, both sorting and repartitioning could potentially reduce the runtime for specific datasets. 
  
  Instead of sorting on both `zipcode` and `income` columns, I could have used the `income` column alone since that would have made it a lot easier for the max_income DataFrame. Adding `zipcode` was an unnecessary step. 
  
  Instead of repartitioning on `zipcode`, I should have utilized the `first_name` column in order to take advantage of the query in the `anna` DataFrame, since we know in advance that the `first_name` column will be filtered. 
  
  If I had more time, and if running was less of a hassle, I might have been able to experiment with more of the combinations and produce optimizing techniques that speed up the running process significantly.
  
  
### Appendix: raw outputs
#### 2.3
#### csv_avg_income
  ##### people_small: 
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:4.575184345245361
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:0.7366690635681152
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:0.644087553024292
	
 ##### people_medium:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:65.86392831802368
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:1.3446431159973145
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:1.184417963027954
	
 ##### people_large:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:30.348625421524048
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:10.751522541046143
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:7.281318426132202

 
 #### csv_max_income
 ##### people_small: 
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:4.502741813659668
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:0.8847708702087402
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:0.7422506809234619

 ##### people_medium:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:8.720243453979492
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:1.2500226497650146
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:1.087057113647461

 ##### people_large:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:18.241158723831177
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:7.2126784324646
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:6.576931715011597

 
 #### csv_anna
 ##### people_small: 
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:2.8158249855041504
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:0.0989372730255127
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_small.csv:0.07913064956665039

 ##### people_medium:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:3.8809494972229004
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:1.1784958839416504
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_medium.csv:0.6338977813720703

 ##### people_large:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:13.335976362228394
	Median Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:5.634191036224365
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/bm106/pub/people_large.csv:5.308367729187012
	
#### 2.4
  #### pq_avg_income
  ##### people_small: 
 	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:3.957742691040039
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:0.9374730587005615
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:0.717667818069458

  ##### people_medium:
 	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:3.7264060974121094
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:0.9269618988037109
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:0.7957730293273926

  ##### people_large:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:11.132874488830566
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:3.6970949172973633
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:3.4880785942077637

  
   #### pq_max_income
   ##### people_small: 
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:3.31514835357666
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:0.7714879512786865
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:0.6363070011138916

   ##### people_medium:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:15.95443058013916
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:4.634495258331299
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:3.739468574523926

   ##### people_large:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:8.756789445877075
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:4.927220106124878
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:4.355566740036011

   
   ### pq_anna
   ##### people_small: 
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_small.parquet:1.651972770690918
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/	people_small.parquet:0.1122734546661377
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/	people_small.parquet:0.08335137367248535

   ##### people_medium:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:1.7626547813415527
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:0.14322161674499512
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_medium.parquet:0.11395144462585449
	
   ##### people_large:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:6.143319368362427
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:1.1996774673461914
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_large.parquet:1.0066728591918945

#### Sort
##### pq_anna:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:3.9205076694488525
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:0.5667850971221924
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:0.49545860290527344
	
  ##### pq_avg_income:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:5.364859580993652
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:0.8156797885894775
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:0.5085346698760986

  ##### pq_max_income:
	Maximum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:10.755784273147583
	Median Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:5.2164716720581055
	Minimum Time taken to run Basic Query 25 times on hdfs:/user/xx2179/people_sorted.parquet:4.0932183265686035

#### Repartition
 ##### pq_anna: 
	Maximum Time taken to run Basic Query 25 times on people_repartition.parquet:36.85057353973389
	Median Time taken to run Basic Query 25 times on people_repartition.parquet:10.103806734085083
	Minimum Time taken to run Basic Query 25 times on people_repartition.parquet:3.2340004444122314

  ##### pq_avg_income:
	Maximum Time taken to run Basic Query 25 times on people_repartition.parquet:58.97018003463745
	Median Time taken to run Basic Query 25 times on people_repartition.parquet:4.140972852706909
	Minimum Time taken to run Basic Query 25 times on people_repartition.parquet:3.6680591106414795



Basic Markdown Guide: https://www.markdownguide.org/basic-syntax/
