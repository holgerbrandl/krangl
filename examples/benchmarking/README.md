
# Benchmarking

Available Libs
* https://scalameter.github.io/
* jmh http://www.baeldung.com/java-microbenchmark-harness --> Seems better


`@State` Model
> When multiple {@link Param}-s are needed for the benchmark run,
 JMH will compute the outer product of all the parameters in the run.

run with json as outformat
```
com.github.holgerbrandl.spark.misc.ExampleBenchmark.init  -rf json -rff results.csv
```

csv provided error is 99.9 CI

if execution plan is injected into benchmark method --> traverse state outer product

Run with
```bash
#/Users/brandl/projects/kotlin/krangl/examples/benchmarking
gradle --info jmh 

sbt 'jmh:run *' ## works if no "extends app" are present in code (see https://github.com/ktoso/sbt-jmh/pull/117#issuecomment-331255198)
#sbt jmh:run -i 3 -wi 3 -f1 -t1 .*FalseSharing.*

# run test benchmark
sbt 'jmh:run -rf json -rff ExampleBenchmark.results.json com.github.holgerbrandl.spark.misc.ExampleBenchmark'

# run local benchmarking
sbt 'jmh:run -rf json -rff threaded_results.json com.github.holgerbrandl.spark.components.ThreadedLabelBM' 
```

For CLI reference see https://github.com/melix/jmh-gradle-plugin#jmh-options-mapping