# Compile and run IntervalJoinBench

## Compile
mvn clean install
mvn package

## Configuration of tests

### Parameters
```
--rate <value>
--sampling <value>
--parallelism <nRSource,nLSource,nJoin,nSink>
--type < su | sz | rd | sd >
[--chaining]
```
Other parameters ( as number of generated keys of synthetic datasets, lower and upper interval's bounds, seeds of generators... ) can be configured through `ij.properties` file located in `/resources`

### Test Types

- **su** = synthetic dataset with uniform distribution
- **sz** = synthetic dataset with zipf distribution
- **rd** = rovio dataset
- **sd** = stock dataset


## Example
``java -jar target/IntervalJoinBench-1.0.jar --rate 0 --sampling 100 --parallelism 1 1 1 1 --type su [--chaining] ``

In the example above, we start the program with parallelism 1 for each operator (Right Source, Left Source, Join, Sink). Latency values are gathered every 100 received tuples in the Sink (sampling parameter) while the generation is performed at full speed (value 0 for the --rate parameter). Test will be performed with a synthetic dataset with uniform distribution (--type `su`).
