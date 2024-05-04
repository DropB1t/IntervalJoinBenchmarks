# Compile and run IntervalJoinBenchmark

## Compile
make all

## Configuration of tests

### Parameters
```
--rate <value>
--sampling <value>
--batch <size>
--parallelism <nRSource,nLSource,nJoin,nSink>
--type < su | sz | rd | sd >
--mode < k | d >
-l <lower bound in ms>
-u <upper bound in ms>
[--chaining]
```

### Test Types

- **su** = synthetic dataset with uniform distribution
- **sz** = synthetic dataset with zipf distribution
- **rd** = rovio dataset
- **sd** = stock dataset

### Parallelism Modes
- **Key based Parallelism**
- **Data based Parallelism** ( with single buffer per stream, per replica )

> In case of Data based Parallelism if you want to      perform buffer size measurements add the `-DWF_JOIN_STATS` flag to `MARCO` variable in `src/Makefile`

## Example
``./bin/ij --rate 0 --sampling 100 --batch 0 --parallelism 1,1,1,1 --type su --mode k -l -500 -u 500 [--chaining] ``

In the example above, we start the program with parallelism 1 for each operator (Right Source, Left Source, Join and Sink separated by commas in the --parallelism attribute).The interval range is computed as [timestamp-500, timestamp+500] in ms precision. Latency values are gathered every 100 received tuples by the Sink (--sampling parameter), while the generation is performed at full speed (value 0 of the --rate parameter). The --batch parameter can be used to apply an output batching from each operator: 0 means no batching, values greater than 0 switch WindFlow in batch mode. After choosing the type of test to perform the generated dataset will have tuples with keys in range from 1 to specified number of keys. The first line of synthetic datasets is formated as `num_keys|data_size`.
