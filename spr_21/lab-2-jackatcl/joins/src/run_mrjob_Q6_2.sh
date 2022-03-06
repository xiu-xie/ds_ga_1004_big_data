#!/bin/bash

module purge
module load python/gcc/3.7.9

python mr_Q6_2.py ../music_small/artist_term.csv ../music_small/track.csv \
        -r hadoop \
        --hadoop-streaming-jar $HADOOP_LIBPATH/$HADOOP_STREAMING \
        --output-dir Q6_2 \
        --python-bin /share/apps/peel/python/3.7.9/gcc/bin/python \
