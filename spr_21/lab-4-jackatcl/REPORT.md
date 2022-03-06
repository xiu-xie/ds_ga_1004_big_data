# Lab 4: Dask

- Name: Long Chen

- NetID: lc3424

## Description of your solution

My approach starts with loading files using a delayed version of the provided GHCN data loader and then flatten the input to only contain desired columns. My observation is that by using a delayed loader function, the process benefits significantly from parallel computing. The data is then converted into a dataframe and is used for later computation. 

I then check data validity by performing checks on the quality and value column as required. The computation is then similar to dataframe operations. One finding is that by using aggregate to calculate mean instead of calling the mean function on the value column, the performance improves significantly. In fact, without aggregation, the script failed on the complete dataset.
