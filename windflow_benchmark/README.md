# Compile and run IntervalJoinBenchmark

## Compile
make all

## Configuration of tests

### Parameters
```
--rate <value>
--key <value>
--sampling <value>
--batch <size>
--parallelism <nRSource,nLSource,nJoin,nSink>
--type < su | sz | rd | sd >
--mode < k | d >
-l (lower bound in ms)
-u (upper bound in ms)
[--chaining]
```

### Test Types

- **su** = synthetic dataset with uniform distribution
- **sz** = synthetic dataset with zipf distribution
- **rd** = rovio dataset
- **sd** = stock dataset

### Parallelism Modes
- **Key based Parallelism**
- **Data based Parallelism** ( with single buffer per stream per replica )

## Example
``./bin/ij --rate 0 --keys 4 --sampling 100 --batch 0 --parallelism 1,1,1,1 --type su --mode k -l -1000 -u 1000 [--chaining] ``

In the example above, we start the program with parallelism 1 for each operator (Right Source, Left Source, Join and Sink separated by commas in the --parallelism attribute). Latency values are gathered every 100 received tuples by the Sink (--sampling parameter), while the generation is performed at full speed (value 0 of the --rate parameter). The --batch parameter can be used to apply an output batching from each operator: 0 means no batching, values greater than 0 switch WindFlow in batch mode. The attribute --keys indicates the number of keys to use: in case the test is of the type **su** or **sz** ( Synthetic test with Uniform Distribution or ZipF Distribution ) the generated dataset will have tuples with keys in range from 1 to specified number of keys ( in the example above is 4 ), otherwise if the type of the test is **rd** or **sd** the --keys attribute i ignored and values of datasets will be used.
