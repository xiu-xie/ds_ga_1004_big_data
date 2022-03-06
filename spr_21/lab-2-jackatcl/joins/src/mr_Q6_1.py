    #! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

class Q6_1(MRJob):
    """
    Multiply two matrices A and B together, returning a result C:

    A * B = C
    """

    def configure_args(self):
        super().configure_args()

    def mapper(self, _, line):
        current_file = jobconf_from_env('mapreduce.map.input.file')
        if 'artist_term.csv' in current_file:
            artist_id, tag = line.strip().split(',')
            yield artist_id, ('TERM', tag)
        elif 'track.csv' in current_file:
            track_id, title, release, year, duration, artist_id = line.strip().split(',')
            if int(year) > 1990:
                yield artist_id, ('TRACK', artist_id, track_id)
        else:
            raise RuntimeError('Could not determine input file!')


    def reducer(self, key, values):
        tags = []
        entries = []
        for value in values:
            if value[0] == 'TERM':
                tags.append(value[1])
            elif value[0] == 'TRACK':
                entries.append((value[1], value[2]))

        for tag in tags:
            for entry in entries:
                yield key, (entry[0], entry[1], tag)


# don't forget the '__name__' == '__main__' block!
if __name__ == '__main__':
    Q6_1.run()
