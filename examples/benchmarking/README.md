
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
# cd /Users/brandl/projects/kotlin/krangl/examples/benchmarking
gradle --info jmh 

sbt 'jmh:run *' ## works if no "extends app" are present in code (see https://github.com/ktoso/sbt-jmh/pull/117#issuecomment-331255198)
#sbt jmh:run -i 3 -wi 3 -f1 -t1 .*FalseSharing.*

# run test benchmark
sbt 'jmh:run -rf json -rff ExampleBenchmark.results.json com.github.holgerbrandl.spark.misc.ExampleBenchmark'

# run local benchmarking
sbt 'jmh:run -rf json -rff threaded_results.json com.github.holgerbrandl.spark.components.ThreadedLabelBM' 
```

For CLI reference see https://github.com/melix/jmh-gradle-plugin#jmh-options-mapping

# Performance Optimization

Memory profiling with JMH

http://java-performance.info/introduction-jmh-profilers/
* no easy to use metric
* HS_GC example
```
HS(GC) | difference: {sun.gc.collector.0.invocations=2, sun.gc.collector.0.lastEntryTime=5589811, sun.gc.collector.0.lastExitTime=5599082,
sun.gc.collector.0.time=21043, sun.gc.compressedclassspace.capacity=262144, sun.gc.compressedclassspace.used=546792,
sun.gc.generation.0.space.0.used=-137562032, sun.gc.generation.0.space.1.used=1310752, sun.gc.generation.1.space.0.used=16384,
sun.gc.metaspace.capacity=1048576, sun.gc.metaspace.used=4734192, sun.gc.policy.avgBaseFootprint=268435456,
sun.gc.policy.avgMinorIntervalTime=1682, sun.gc.policy.avgMinorPauseTime=6, sun.gc.policy.avgPromotedAvg=-268427264,
sun.gc.policy.avgPromotedPaddedAvg=-268427264, sun.gc.policy.avgSurvivedAvg=-43057136, sun.gc.policy.avgSurvivedDev=98296,
sun.gc.policy.avgSurvivedPaddedAvg=-42762248, sun.gc.policy.avgYoungLive=1507344, sun.gc.policy.desiredSurvivorSize=44564480,
sun.gc.policy.liveSpace=269942784, sun.gc.policy.minorPauseTime=5, sun.gc.policy.mutatorCost=98,
sun.gc.policy.oldCapacity=-357564416, sun.gc.policy.promoted=8192, sun.gc.policy.survived=1310752, sun.gc.policy.tenuringThreshold=-8,
sun.gc.tlab.alloc=34171776, sun.gc.tlab.allocThreads=2, sun.gc.tlab.fills=53, sun.gc.tlab.gcWaste=671086, sun.gc.tlab.maxFills=52,
sun.gc.tlab.maxGcWaste=671086, sun.gc.tlab.maxSlowWaste=220, sun.gc.tlab.slowWaste=220}
```

https://cruftex.net/2017/03/28/The-6-Memory-Metrics-You-Should-Track-in-Your-Java-Benchmarks.html?pk_campaign=jmh


* https://stackoverflow.com/questions/22640804/count-metrics-with-jmh --> use HS_GC

https://stackoverflow.com/questions/42917365/jmh-setting-lines-parameter-for-stack-profiler-programmatically -> no annotation way to set profiler:

https://stackoverflow.com/questions/19785290/java-unit-testing-how-to-measure-memory-footprint-for-method-call or avoid jmh and rather use simplisitic `    Runtime runtime = Runtime.getRuntime();
; memoryBefore = runtime.totalMemory() - runtime.freeMemory()`


MOst detailed thread http://mail.openjdk.java.net/pipermail/jmh-dev/2017-April/002545.html


## Reference Benchmarks

Mainly interesting for eval matrix design

https://github.com/fabienrenaud/java-json-benchmark