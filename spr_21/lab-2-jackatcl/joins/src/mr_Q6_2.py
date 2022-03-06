#! /usr/bin/env python

from mrjob.job import MRJob, MRStep
from mrjob.compat import jobconf_from_env


class Q6_2_1(MRJob):
    """
    Multiply two matrices A and B together, returning a result C:

    A * B = C
    """

    def configure_args(self):
        super().configure_args()

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer1),
            MRStep(reducer=self.reducer2)
        ]

    def mapper(self, _, line):
        current_file = jobconf_from_env('mapreduce.map.input.file')
        if 'artist_term.csv' in current_file:
            artist_id, tag = line.strip().split(',')
            yield artist_id, ('TERM', tag)
        elif 'track.csv' in current_file:
            track_id, title, release, year, duration, artist_id = line.strip().split(',')
            yield artist_id, ('TRACK', artist_id, int(year), float(duration))
        else:
            raise RuntimeError('Could not determine input file!')

    def reducer1(self, key, values):
        tags = []
        entries = []
        for value in values:
            if value[0] == 'TERM':
                tags.append(value[1])
            else:
                entries.append((value[1], value[2], value[3]))

        for tag in tags:
            for entry in entries:
                yield tag, (tag, entry[0], entry[1], entry[2])

    def reducer2(self, key, values):
        year = []
        duration = []
        artist_id = set()
        for value in values:
            year.append(value[2])
            duration.append(value[3])
            artist_id.add(value[1])

        yield key, (key, min(year), sum(duration) / len(duration), len(artist_id))


# don't forget the '__name__' == '__main__' block!
if __name__ == '__main__':
    Q6_2_1.run()
