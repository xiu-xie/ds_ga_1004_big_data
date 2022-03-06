#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# USAGE:
#   python Lab1.py Sample_Song_Dataset.db

import sys
import sqlite3


# The database file should be given as the first argument on the command line
# Please do not hard code the database file!
db_file = sys.argv[1]

# We connect to the database using 
with sqlite3.connect(db_file) as conn:
    # We use a "cursor" to mark our place in the database.
    # We could use multiple cursors to keep track of multiple
    # queries simultaneously.
    cursor = conn.cursor()

    # This query counts the number of tracks from the year 1998
    year = ('1998',)
    cursor.execute('SELECT count(*) FROM tracks WHERE year=?', year)

    # Since there is no grouping here, the aggregation is over all rows
    # and there will only be one output row from the query, which we can
    # print as follows:
    print('Tracks from {}: {}'.format(year[0], cursor.fetchone()[0]))
    # The [0] bits here tell us to pull the first column out of the 'year' tuple
    # and query results, respectively.

    # ADD YOUR CODE STARTING HERE

    cursor.execute('SELECT tracks.artist_id, tracks.track_id, artist_term.term FROM tracks INNER JOIN artist_term ON tracks.artist_id = artist_term.artist_id WHERE tracks.year > 1990')
    q1_ans = cursor.fetchall()
    c = 0
    for res in q1_ans:
        c += 1
        # print(res)

    print(c)

    cursor.execute('SELECT artist_term.term, min(tracks.year), avg(tracks.duration), count(artist_term.artist_id) FROM tracks LEFT JOIN artist_term ON tracks.artist_id = artist_term.artist_id GROUP BY artist_term.term')
    q1_ans = cursor.fetchall()
    c = 0
    for res in q1_ans:
        c += 1
        print(res)

    print(c)

    # # Q1
    # print('Q1' + '*' * 100)
    # q1_track_title = ('Silent Night (Album)', )
    # cursor.execute('SELECT tracks.artist_id, artist_name, artist_term.term FROM tracks LEFT JOIN artists on tracks.artist_id=artists.artist_id LEFT JOIN artist_term on tracks.artist_id=artist_term.artist_id WHERE tracks.title=?', q1_track_title)
    # q1_ans = cursor.fetchall()
    # for artist_id, artist_name, artist_term in q1_ans:
    #     print('{:20s}    {:20s}    {:20s}'.format(artist_id, artist_name, artist_term))
    #
    # # Q2
    # print('Q2' + '*' * 100)
    # cursor.execute('SELECT DISTINCT title FROM tracks WHERE tracks.duration < 3')
    # q2_ans = cursor.fetchall()
    # for res in q2_ans:
    #     print(res[0])
    #
    #
    # # Q3
    # print('Q3' + '*' * 100)
    # cursor.execute('SELECT track_id FROM tracks WHERE year>? AND year<? ORDER BY duration DESC limit 10', (2008, 2012, ))
    # q3_ans = cursor.fetchall()
    # for res in q3_ans:
    #     print(res[0])
    #
    # # Q4
    # print('Q4' + '*' * 100)
    # cursor.execute('SELECT term FROM artist_term GROUP BY term ORDER BY COUNT(term), term ASC limit 15')
    # q4_ans = cursor.fetchall()
    # for res in q4_ans:
    #     print(res[0])
    #
    #
    # # Q5
    # print('Q5' + '*' * 100)
    # cursor.execute('SELECT artists.artist_name FROM tracks LEFT JOIN artists ON tracks.artist_id=artists.artist_id ORDER BY duration ASC limit 1')
    # q5_ans = cursor.fetchall()
    # for res in q5_ans:
    #     print(res[0])
    #
    #
    # # Q6
    # print('Q6' + '*' * 100)
    # cursor.execute('SELECT AVG(duration) FROM tracks')
    # q6_ans = cursor.fetchall()
    # for res in q6_ans:
    #     print('mean={}'.format(res[0]))
    #
    #
    # # Q7
    # print('Q7' + '*' * 100)
    # cursor.execute('SELECT t.track_id FROM artist_term as artt LEFT JOIN tracks as t ON t.artist_id=artt.artist_id GROUP BY t.artist_id HAVING COUNT(artt.term) > 8 ORDER BY t.track_id ASC limit 10')
    # q7_ans = cursor.fetchall()
    # for res in q7_ans:
    #     print(res[0])
    #
    #
    # # Q8
    # print('Q8' + '*' * 100)
    # import time
    # q8_timer_no_idx = []
    # for _ in range(100):
    #     start = time.time()
    #     cursor.execute('SELECT tracks.artist_id, artist_name, artist_term.term FROM tracks LEFT JOIN artists on tracks.artist_id=artists.artist_id LEFT JOIN artist_term on tracks.artist_id=artist_term.artist_id WHERE tracks.title=?',q1_track_title)
    #     end = time.time()
    #     q8_timer_no_idx.append(end - start)
    #
    # q8_timer_with_idx = []
    # sql = ("CREATE INDEX IF NOT EXISTS idx_track ON tracks (artist_id)")
    # cursor.execute(sql)
    # sql = ("CREATE INDEX IF NOT EXISTS idx_artist ON artists (artist_id)")
    # cursor.execute(sql)
    # sql = ("CREATE INDEX IF NOT EXISTS idx_term ON artist_term (artist_id)")
    # cursor.execute(sql)
    #
    # for _ in range(100):
    #     start = time.time()
    #     cursor.execute('SELECT tracks.artist_id, artist_name, artist_term.term FROM tracks LEFT JOIN artists on tracks.artist_id=artists.artist_id LEFT JOIN artist_term on tracks.artist_id=artist_term.artist_id WHERE tracks.title=?',q1_track_title)
    #     end = time.time()
    #     q8_timer_with_idx.append(end - start)
    #
    # # print(q8_timer_with_idx)
    # print('Without indexing: {}\nWith indexing: {}'.format(min(q8_timer_no_idx), min(q8_timer_with_idx)))
    #
    # # Without indexing: 0.9409310817718506
    # # With indexing: 0.10268712043762207
    #
    # # Q9
    # print('Q9' + '*' * 100)
    # cursor.execute('BEGIN TRANSACTION')
    # cursor.execute('DELETE FROM tracks WHERE artist_id IN (SELECT artist_term.artist_id FROM tracks LEFT JOIN artist_term ON tracks.artist_id=artist_term.artist_id WHERE artist_term.term=?)', ('europop',))
    # cursor.fetchall()
    # cursor.execute('SELECT COUNT(*) FROM tracks')
    # count_before_rollback = cursor.fetchall()
    # cursor.execute('ROLLBACK')
    # cursor.fetchall()
    # cursor.execute('SELECT COUNT(*) FROM tracks')
    # count_after_rollback = cursor.fetchall()
    #
    # print('Number of tracks before ROLLBACK: {}\nNumber of tracks after ROLLBACK: {}'.format(count_before_rollback[0][0], count_after_rollback[0][0]))
    #