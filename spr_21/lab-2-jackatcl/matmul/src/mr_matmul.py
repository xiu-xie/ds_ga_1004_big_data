    #! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env


class MRMatrixMultiplication(MRJob):
    """
    Multiply two matrices A and B together, returning a result C:

    A * B = C
    """

    def configure_args(self):
        super().configure_args()

        self.add_passthru_arg('--result-colsize',
                              required=True,
                              type=int,
                              help=('Result matrix\'s number of columns.'
                                    ' Required.'))

        self.add_passthru_arg('--result-rowsize',
                              required=True,
                              type=int,
                              help=('Result matrix\'s number of rows.'
                                    ' Required.'))

    def mapper(self, _, line):
        """
        Implement your mapper here!

        
        
        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) 
                key: str
                value: int
                You have to design the intermediate keys and values shape yourself,
                and then use them appropriately in the reducer.
        """

        READING_FROM_A = False
        READING_FROM_B = False

        # find out what file we're currently parsing
        current_file = jobconf_from_env('mapreduce.map.input.file')
        if 'A.txt' in current_file:
            READING_FROM_A = True
        elif 'B.txt' in current_file:
            READING_FROM_B = True
        else:
            raise RuntimeError('Could not determine input file!')

        number_of_rows = self.options.result_rowsize  # C row count
        number_of_cols = self.options.result_colsize  # C column count

        # use line to yield necessary key-value pairs
        row, col, val = line.strip().split(',')

        if READING_FROM_A:
            for i in range(number_of_cols):
                key = (int(row), i)
                yield key, ('A', int(col), float(val))
        if READING_FROM_B:
            for i in range(number_of_rows):
                key = (i, int(col))
                yield key, ('B', int(row), float(val))

    # optional: implement the combiner:
    # def combiner(self, key, values):
        # start using the key-value pairs to calculate the result matrix
        # pass

    def reducer(self, key, values):
        """
        Implement your reducer here!

        This will accept a (key, list(values)) and calculate the result
        matrix C.
        
        Parameters:
            key: str
                same as the key defined in the mapper
            values: list
                list containing values corresponding to the key defined in the mapper

            Yields:
                key: str
                    same as the key defined in the mapper
                value: int
                    value corresponding to each key. This is the resulting matrix C
        """
        # use the key-value pairs to calculate the result matrix
        ans = 0

        # create partial A and B
        A = []
        B = []

        for value in values:
            if value[0] == 'A':
                A.append((value[1], value[2]))
            else:
                B.append((value[1], value[2]))

        for a in A:
            for b in B:
                if a[0] == b[0]: # need to be multiplied and added to ans
                    ans += a[1] * b[1]

        # only record non-zero entries
        if ans:
            yield key, ans


# don't forget the '__name__' == '__main__' block!
if __name__ == '__main__':
    MRMatrixMultiplication.run()
